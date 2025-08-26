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
 * - Uses per-shard BufferedRunner to buffer and process sequentially
 * - Computes Demand from runner idleness (1 if queue empty + idle, else 0)
 * - Commits on a simple time tick
 */
public final class GenericWorker<M, S, P> implements Runnable {
    private final String topic;
    private final FlowControlledStream<M, S, P> source;
    private final RecordAccess<M, S, P, ?, ?> access;
    private final MessageProcessor<M, S> processor;
    private final NextPosition<M, P> nextPosition;
    private final MetricsRegistry<S> metrics;
    private final int pollMs;
    private final long commitTickMs;
    private final ThreadFactory laneFactory;
    private final int runnerBufferCapacity;

    private final Set<S> assigned = ConcurrentHashMap.newKeySet();
    private final Map<S, PartitionRunner<M, S, P>> runners = new ConcurrentHashMap<>();
    private final SeekStrategy<S, P> seekStrategy;
    private final SeekOps<S, P> seekOps;

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
                         int runnerBufferCapacity) {
        this.topic = Objects.requireNonNull(topic);
        this.source = Objects.requireNonNull(source);
        this.access = Objects.requireNonNull(access);
        this.processor = Objects.requireNonNull(processor);
        this.nextPosition = Objects.requireNonNull(nextPosition);
        this.metrics = metrics;
        this.seekStrategy = Objects.requireNonNullElse(seekStrategy, SeekStrategy.continueCommitted());
        this.seekOps = Objects.requireNonNull(seekOps, "seekOps");
        this.pollMs = Math.max(1, pollMs);
        this.commitTickMs = Math.max(10, commitTickMs);
        this.laneFactory = Objects.requireNonNull(laneFactory);
        this.runnerBufferCapacity = Math.max(1, runnerBufferCapacity);
    }

    public void requestStop() {
        running = false;
    }

    @Override
    public void run() {
        source.subscribe(topic, new AssignmentListener<>() {
            @Override
            public void onAssigned(Set<S> shards) {
                // Track shards and ensure runners exist
                for (S s : shards) {
                    assigned.add(s);
                    runners.computeIfAbsent(s, k ->
                            new BufferedRunner<>(k, processor, metrics,
                                    namedLaneFactory("lane-" + safeName(k)),
                                    runnerBufferCapacity)
                    );
                }
                // >>> Apply seek policy for newly assigned shards
                // Do this AFTER registering shards/runners but BEFORE the next poll
                // so the next fetch starts at the desired position.
                if (!shards.isEmpty()) {
                    seekStrategy.onAssigned(seekOps, shards);
                }
            }

            @Override
            public void onRevoked(Set<S> shards) {
                for (S s : shards) {
                    assigned.remove(s);
                    PartitionRunner<M, S, P> r = runners.remove(s);
                    if (r != null) r.stopAndDrain(10_000);
                }
            }
        });

        long lastCommit = System.currentTimeMillis();

        try {
            while (running) {
                Map<S, Integer> perShard = new HashMap<>();
                int globalMax = 0;
                for (S s : assigned) {
                    var r = runners.get(s);
                    if (r == null) continue;
                    int rem = r.remainingCapacity();
                    if (rem > 0) { perShard.put(s, rem); globalMax += rem; }
                }
                Demand<S> demand = Demand.of(perShard, globalMax);
                Iterable<M> batch = source.poll(Duration.ofMillis(pollMs), demand);

                for (M msg : batch) {
                    S shard = access.shardOf(msg);
                    P next = nextPosition.of(msg);
                    PartitionRunner<M, S, P> r = runners.get(shard);
                    if (r != null) r.tryStart(msg, next);
                }

                // periodic commit
                long now = System.currentTimeMillis();
                if (now - lastCommit >= commitTickMs) {
                    Map<S, P> commitMap = new HashMap<>();
                    for (var e : runners.entrySet()) {
                        P next = e.getValue().commitReadyNext();
                        if (next != null) commitMap.put(e.getKey(), next);
                    }
                    if (!commitMap.isEmpty()) {
                        try {
                            source.commit(commitMap);
                        } catch (Exception ignored) {
                        }
                    }
                    lastCommit = now;
                }
            }
        } finally {
            try {
                Map<S, P> commitMap = new HashMap<>();
                for (var e : runners.entrySet()) {
                    P next = e.getValue().commitReadyNext();
                    if (next != null) commitMap.put(e.getKey(), next);
                }
                if (!commitMap.isEmpty()) source.commit(commitMap);
            } catch (Exception ignored) {
            }
            for (var r : runners.values()) {
                try {
                    r.stopAndDrain(5_000);
                } catch (Exception ignored) {
                }
            }
            runners.clear();
            try {
                source.close();
            } catch (Exception ignored) {
            }
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
