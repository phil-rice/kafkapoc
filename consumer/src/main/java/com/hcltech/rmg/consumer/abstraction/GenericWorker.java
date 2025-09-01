package com.hcltech.rmg.consumer.abstraction;

import com.hcltech.rmg.metrics.MetricsRegistry;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class GenericWorker<M, S, P> implements Runnable {
    private final String topic;
    private final FlowControlledStream<M, S, P> source;
    private final RecordAccess<M, S, P, ?, ?> access;
    private final MessageProcessor<M, S> processor;
    private final NextPosition<M, P> nextPosition;
    private final MetricsRegistry<S> metrics; // nullable

    private final SeekStrategy<S, P> seekStrategy;
    private final SeekOps<S, P> seekOps;

    private final int pollMs;
    private final long commitTickMs;
    private final ThreadFactory laneFactory;
    private final int runnerBufferCapacity;

    private final DemandCalculator<S, ? extends RunnerState> calculator;

    private final Set<S> assigned = ConcurrentHashMap.newKeySet();
    private final Map<S, PartitionRunner<M, S, P>> runners = new ConcurrentHashMap<>();

    private volatile boolean running = true;

    // Primary ctor (pass calculator explicitly)
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
                         DemandCalculator<S, ? extends RunnerState> calculator) {
        this.topic = Objects.requireNonNull(topic);
        this.source = Objects.requireNonNull(source);
        this.access = Objects.requireNonNull(access);
        this.processor = Objects.requireNonNull(processor);
        this.nextPosition = Objects.requireNonNull(nextPosition);
        this.metrics = metrics;
        this.seekStrategy = Objects.requireNonNullElse(seekStrategy, SeekStrategy.continueCommitted());
        this.seekOps = Objects.requireNonNull(seekOps);
        this.pollMs = Math.max(1, pollMs);
        this.commitTickMs = Math.max(10, commitTickMs);
        this.laneFactory = Objects.requireNonNull(laneFactory);
        this.runnerBufferCapacity = Math.max(1, runnerBufferCapacity);
        this.calculator = Objects.requireNonNull(calculator);
    }

    // Back-compat ctor: reproduces current behavior
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
        this(topic, source, access, processor, nextPosition, metrics, seekStrategy, seekOps,
                pollMs, commitTickMs, laneFactory, runnerBufferCapacity,
                new CapacityTopUpCalculator<>(
                        new DemandCalculatorConfig(perPollPerPartition, maxOutstandingPerPartition, 0),
                        (processor instanceof ProcessorProfile<?> pp) ? (ProcessorProfile<S>) pp : null
                )
        );
    }

    // Convenience ctor
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

    private void mset(String name, S shard, long v) {
        if (metrics != null) metrics.set(name, shard, v);
    }

    @Override
    public void run() {
        source.subscribe(topic, new AssignmentListener<>() {
            @Override public void onAssigned(Set<S> shards) {
                for (S s : shards) {
                    assigned.add(s);
                    runners.computeIfAbsent(s, k ->
                            new BufferedRunner<>(k, processor, metrics,
                                    namedLaneFactory("lane-" + safeName(k)),
                                    runnerBufferCapacity)
                    );
                }
                if (!shards.isEmpty()) seekStrategy.onAssigned(seekOps, shards);
            }
            @Override public void onRevoked(Set<S> shards) {
                for (S s : shards) {
                    assigned.remove(s);
                    PartitionRunner<M, S, P> r = runners.remove(s);
                    if (r != null) { try { r.stopAndDrain(10_000); } catch (Exception ignored) {} }
                }
            }
        });

        long lastCommit = System.currentTimeMillis();

        try {
            while (running) {
                // Build RunnerState view
                Map<S, RunnerState> view = new HashMap<>();
                for (S s : assigned) {
                    PartitionRunner<M, S, P> r = runners.get(s);
                    if (r == null) continue;
                    final int rem = Math.max(0, r.remainingCapacity());
                    final int out = Math.max(0, r.outstanding());
                    view.put(s, new RunnerState() {
                        @Override public int remainingCapacity() { return rem; }
                        @Override public int outstanding()       { return out; }
                    });
                }

                // Compute demand
                @SuppressWarnings("unchecked")
                Demand<S> demand = ((DemandCalculator<S, RunnerState>) calculator).compute(assigned, view);

                // Minimal PRE gauges (need/queued/inflight)
                for (S s : assigned) {
                    RunnerState rs = view.get(s);
                    if (rs == null) { mset("runner.need", s, 0); mset("runner.queued", s, 0); mset("runner.inflight", s, 0); continue; }
                    int need = demand.perShard().getOrDefault(s, 0);
                    int queued   = Math.max(0, runnerBufferCapacity - rs.remainingCapacity());
                    int inflight = Math.max(0, rs.outstanding() - queued);
                    mset("runner.need",     s, need);
                    mset("runner.queued",   s, queued);
                    mset("runner.inflight", s, inflight);
                }

                // Poll & enqueue
                Iterable<M> batch = source.poll(Duration.ofMillis(pollMs), demand);
                for (M msg : batch) {
                    S shard = access.shardOf(msg);
                    P next  = nextPosition.of(msg);
                    PartitionRunner<M, S, P> r = runners.get(shard);
                    if (r != null) r.tryStart(msg, next);
                }

                // Commit tick
                long now = System.currentTimeMillis();
                if (now - lastCommit >= commitTickMs) {
                    Map<S, P> commitMap = new HashMap<>();
                    for (var e : runners.entrySet()) {
                        P next = e.getValue().commitReadyNext();
                        if (next != null) commitMap.put(e.getKey(), next);
                    }
                    if (!commitMap.isEmpty()) { try { source.commit(commitMap); } catch (Exception ignored) {} }
                    lastCommit = now;
                }
            }
        } finally {
            for (var r : runners.values()) { try { r.stopAndDrain(30_000); } catch (Exception ignored) {} }
            try {
                Map<S, P> commitMap = new HashMap<>();
                for (var e : runners.entrySet()) {
                    P next = e.getValue().commitReadyNext();
                    if (next != null) commitMap.put(e.getKey(), next);
                }
                if (!commitMap.isEmpty()) source.commit(commitMap);
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
