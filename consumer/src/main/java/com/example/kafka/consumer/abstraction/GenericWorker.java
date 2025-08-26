package com.example.kafka.consumer.abstraction;

import com.example.metrics.MetricsRegistry;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generic worker that:
 * - Subscribes to one topic
 * - Spawns one ordered PartitionRunner per shard (partition)
 * - Uses CREDIT-BASED flow control driven by per-partition remaining capacity
 * - Emits minimal PRE gauges: runner.need, runner.queued, runner.inflight
 * - Commits periodically on a simple time tick
 *
 * <M> message type
 * <S> shard id type (e.g., TopicPartition)
 * <P> position type (e.g., next offset to commit)
 */
public final class GenericWorker<M, S, P> implements Runnable {
    private final String topic;
    private final FlowControlledStream<M, S, P> source;
    private final RecordAccess<M, S, P, ?, ?> access;
    private final MessageProcessor<M, S> processor;
    private final NextPosition<M, P> nextPosition;
    private final MetricsRegistry<S> metrics; // may be null

    private final SeekStrategy<S, P> seekStrategy;
    private final SeekOps<S, P> seekOps;

    private final int pollMs;
    private final long commitTickMs;
    private final ThreadFactory laneFactory;
    private final int runnerBufferCapacity;

    /** Flow control knobs */
    private final int maxOutstandingPerPartition; // hard credit ceiling per partition
    private final int perPollPerPartition;       // per-poll top-up cap per partition

    private final Set<S> assigned = ConcurrentHashMap.newKeySet();
    private final Map<S, PartitionRunner<M, S, P>> runners = new ConcurrentHashMap<>();

    private volatile boolean running = true;

    public GenericWorker(String topic,
                         FlowControlledStream<M, S, P> source,
                         RecordAccess<M, S, P, ?, ?> access,
                         MessageProcessor<M, S> processor,
                         NextPosition<M, P> nextPosition,
                         MetricsRegistry<S> metrics,
                         SeekStrategy<S, P> seekStrategy,
                         SeekOps<S, P> seekOps,
                         int pollMs,
                         long commitTickMs,
                         ThreadFactory laneFactory,
                         int runnerBufferCapacity,
                         int maxOutstandingPerPartition,
                         int perPollPerPartition) {
        this.topic = Objects.requireNonNull(topic, "topic");
        this.source = Objects.requireNonNull(source, "source");
        this.access = Objects.requireNonNull(access, "access");
        this.processor = Objects.requireNonNull(processor, "processor");
        this.nextPosition = Objects.requireNonNull(nextPosition, "nextPosition");
        this.metrics = metrics;

        this.seekStrategy = Objects.requireNonNullElse(seekStrategy, SeekStrategy.continueCommitted());
        this.seekOps = Objects.requireNonNull(seekOps, "seekOps");

        this.pollMs = Math.max(1, pollMs);
        this.commitTickMs = Math.max(10, commitTickMs);
        this.laneFactory = Objects.requireNonNull(laneFactory, "laneFactory");
        this.runnerBufferCapacity = Math.max(1, runnerBufferCapacity);

        this.maxOutstandingPerPartition = Math.max(1, maxOutstandingPerPartition);
        this.perPollPerPartition = Math.max(1, perPollPerPartition);
    }

    /** Convenience ctor: derives sensible credits from buffer capacity. */
    public GenericWorker(String topic,
                         FlowControlledStream<M, S, P> source,
                         RecordAccess<M, S, P, ?, ?> access,
                         MessageProcessor<M, S> processor,
                         NextPosition<M, P> nextPosition,
                         MetricsRegistry<S> metrics,
                         SeekStrategy<S, P> seekStrategy,
                         SeekOps<S, P> seekOps,
                         int pollMs,
                         long commitTickMs,
                         ThreadFactory laneFactory,
                         int runnerBufferCapacity) {
        this(topic, source, access, processor, nextPosition, metrics, seekStrategy, seekOps,
                pollMs, commitTickMs, laneFactory, runnerBufferCapacity,
                /* maxOutstandingPerPartition */ Math.max(1, runnerBufferCapacity) * 10,
                /* perPollPerPartition      */  Math.max(1, runnerBufferCapacity));
    }

    public void requestStop() { running = false; }

    // ---------------- Metrics helpers (PRE only) ----------------

    private void mset(String name, S shard, long value) {
        if (metrics != null) metrics.set(name, shard, value);
    }

    /** Publish zeros for the minimal gauges when runner is missing. */
    private void publishZeroPreGauges(S shard) {
        mset("runner.need",     shard, 0);
        mset("runner.queued",   shard, 0);
        mset("runner.inflight", shard, 0);
    }

    /**
     * Publish PRE gauges and compute 'need' for this shard.
     * queued = bufferCapacity - remainingCapacity
     * inflight = max(0, outstanding - queued)  // 0 or 1 for single-lane
     */
    private int publishPreGaugesAndComputeNeed(S shard, PartitionRunner<M, S, P> r) {
        int remCap      = Math.max(0, r.remainingCapacity());
        int outstanding = Math.max(0, r.outstanding());
        int queued      = Math.max(0, runnerBufferCapacity - remCap);
        int inflight    = Math.max(0, outstanding - queued);

        int credit      = Math.max(0, maxOutstandingPerPartition - outstanding);
        int need        = (remCap > 0 && credit > 0)
                ? Math.min(remCap, Math.min(perPollPerPartition, credit))
                : 0;

        mset("runner.need",     shard, need);
        mset("runner.queued",   shard, queued);
        mset("runner.inflight", shard, inflight);

        return need;
    }

    // ---------------- Run loop ----------------

    @Override
    public void run() {
        // Subscribe; create runners; then apply seek policy
        source.subscribe(topic, new AssignmentListener<>() {
            @Override
            public void onAssigned(Set<S> shards) {
                for (S s : shards) {
                    assigned.add(s);
                    runners.computeIfAbsent(s, k ->
                            new BufferedRunner<>(k, processor, metrics,
                                    namedLaneFactory("lane-" + safeName(k)),
                                    runnerBufferCapacity)
                    );
                }
                if (!shards.isEmpty()) {
                    seekStrategy.onAssigned(seekOps, shards);
                }
            }

            @Override
            public void onRevoked(Set<S> shards) {
                for (S s : shards) {
                    assigned.remove(s);
                    PartitionRunner<M, S, P> r = runners.remove(s);
                    if (r != null) {
                        try { r.stopAndDrain(10_000); } catch (Exception ignored) {}
                    }
                }
            }
        });

        long lastCommit = System.currentTimeMillis();

        try {
            while (running) {
                // Build per-shard credits and publish PRE gauges
                Map<S, Integer> perShard = new HashMap<>();
                int globalMax = 0;

                for (S s : assigned) {
                    PartitionRunner<M, S, P> r = runners.get(s);
                    if (r == null) {
                        publishZeroPreGauges(s);
                        continue;
                    }
                    int need = publishPreGaugesAndComputeNeed(s, r);
                    if (need > 0) {
                        perShard.put(s, need);
                        globalMax += need;
                    }
                }

                Demand<S> demand = (globalMax > 0) ? Demand.of(perShard, globalMax)
                        : Demand.of(Map.of(), 0);

                // Fetch from stream according to our credits
                Iterable<M> batch = source.poll(Duration.ofMillis(pollMs), demand);

                // Enqueue into runners (should not fail with remainingCapacity-based demand)
                for (M msg : batch) {
                    S shard = access.shardOf(msg);
                    P next  = nextPosition.of(msg);
                    PartitionRunner<M, S, P> r = runners.get(shard);
                    if (r != null) {
                        boolean ok = r.tryStart(msg, next);
                        // If false, our demand math is off; consider logging in your codebase.
                    }
                }

                // Periodic commit
                long now = System.currentTimeMillis();
                if (now - lastCommit >= commitTickMs) {
                    Map<S, P> commitMap = new HashMap<>();
                    for (var e : runners.entrySet()) {
                        P next = e.getValue().commitReadyNext();
                        if (next != null) commitMap.put(e.getKey(), next);
                    }
                    if (!commitMap.isEmpty()) {
                        try { source.commit(commitMap); } catch (Exception ignored) {}
                    }
                    lastCommit = now;
                }
            }
        } finally {
            // Graceful shutdown: stop lanes → final commit → close source
            for (var r : runners.values()) {
                try { r.stopAndDrain(30_000); } catch (Exception ignored) {}
            }
            try {
                Map<S, P> commitMap = new HashMap<>();
                for (var e : runners.entrySet()) {
                    P next = e.getValue().commitReadyNext();
                    if (next != null) commitMap.put(e.getKey(), next);
                }
                if (!commitMap.isEmpty()) {
                    source.commit(commitMap);
                }
            } catch (Exception ignored) {}
            runners.clear();
            try { source.close(); } catch (Exception ignored) {}
        }
    }

    private ThreadFactory namedLaneFactory(String base) {
        AtomicInteger idx = new AtomicInteger(0);
        return r -> {
            Thread t = laneFactory.newThread(r);
            t.setName(base + "-" + idx.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    private static String safeName(Object shard) {
        return String.valueOf(shard).replaceAll("[^a-zA-Z0-9._-]", "_");
    }
}
