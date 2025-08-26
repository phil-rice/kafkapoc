package com.example.kafka.consumer.abstraction;

import com.example.metrics.MetricsRegistry;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Runner that buffers a batch of messages, but still processes them sequentially.
 * Ordering per shard is preserved. At most one processing task runs at a time.
 *
 * @param <M> message type
 * @param <S> shard id type
 * @param <P> position type
 */
public final class BufferedRunner<M, S, P> implements PartitionRunner<M, S, P> {
    private final S shard;
    private final MessageProcessor<M, S> processor;
    private final ExecutorService lane; // one lane per shard

    private final MetricsRegistry metrics;
    /** Local queue of messages pulled in one poll batch. */
    private final BlockingQueue<Entry<M, P>> queue;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean busy = new AtomicBoolean(false);

    /** Last contiguous commit-ready position (or null if none). */
    private volatile P commitNext = null;

    public BufferedRunner(S shard,
                          MessageProcessor<M, S> processor,
                          MetricsRegistry metrics,
                          ThreadFactory threadFactory,
                          int capacity) {
        this.shard = Objects.requireNonNull(shard);
        this.processor = Objects.requireNonNull(processor);
        this.metrics = metrics;
        this.queue = new ArrayBlockingQueue<>(Math.max(1, capacity));
        this.lane = Executors.newSingleThreadExecutor(threadFactory);
        this.lane.execute(this::loop);
    }

    @Override
    public boolean isIdle() {
        // We only want new demand when both queue is empty AND not busy
        return queue.isEmpty() && !busy.get();
    }

    @Override
    public boolean tryStart(M message, P next) {
        // Push message into local buffer. Returns false if buffer full.
        return queue.offer(new Entry<>(message, next));
    }

    @Override
    public P commitReadyNext() {
        return commitNext;
    }

    @Override
    public void stopAndDrain(long timeoutMs) {
        running.set(false);
        lane.shutdown();
        try {
            lane.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    // worker loop: sequentially drains queue
    private void loop() {
        try {
            while (running.get() || !queue.isEmpty()) {
                Entry<M, P> e = queue.poll(100, TimeUnit.MILLISECONDS);
                if (e == null) continue;

                busy.set(true);
                try {
                    processor.process(shard, e.msg);
                    metrics.recordProcessed(shard);
                    commitNext = e.next;
                } catch (Exception ex) {
                    // failed; do not advance commitNext
                    // you could log / DLQ here
                } finally {
                    busy.set(false);
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
    @Override public int remainingCapacity() { return queue.remainingCapacity(); }

    private record Entry<M, P>(M msg, P next) {}
}
