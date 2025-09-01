package com.hcltech.rmg.consumer.abstraction;

import com.hcltech.rmg.metrics.MetricsRegistry;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Buffered, per-partition runner.
 * - Processes messages sequentially on a single lane thread per shard.
 * - Maintains an "outstanding" count (queued + in-flight).
 * - Exposes a commit watermark (commitNext) that advances only on successful processing.
 *
 * @param <M> message type
 * @param <S> shard id type
 * @param <P> position type (e.g., next offset)
 */
public final class BufferedRunner<M, S, P> implements PartitionRunner<M, S, P> {

    private final S shard;
    private final MessageProcessor<M, S> processor;
    private final MetricsRegistry<S> metrics;

    /** Single-thread executor = one lane per shard (preserves order). */
    private final ExecutorService lane;

    /** Local queue of work items for this shard. */
    private final BlockingQueue<Entry<M, P>> queue;

    /** Running flag for the lane loop. */
    private final AtomicBoolean running = new AtomicBoolean(true);

    /** Number of items outstanding (queued + currently processing). */
    private final AtomicInteger outstanding = new AtomicInteger(0);

    /** Commit watermark: next position that is safe to commit (contiguous success). */
    private volatile P commitNext = null;

    public BufferedRunner(S shard,
                          MessageProcessor<M, S> processor,
                          MetricsRegistry<S> metrics,
                          ThreadFactory threadFactory,
                          int capacity) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.processor = Objects.requireNonNull(processor, "processor");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.queue = new ArrayBlockingQueue<>(Math.max(1, capacity));
        this.lane = Executors.newSingleThreadExecutor(Objects.requireNonNull(threadFactory, "threadFactory"));
        this.lane.execute(this::loop);
    }

    /** Convenience: completely idle when nothing is queued or in-flight. */
    @Override
    public boolean isIdle() {
        return outstanding.get() == 0;
    }

    /** Number of items currently queued + in-flight. Useful for credit-based demand. */
    public int outstanding() {
        return outstanding.get();
    }

    /** Remaining queue capacity (how many more we can accept without blocking). */
    public int remainingCapacity() {
        return queue.remainingCapacity();
    }

    /** Try to enqueue a message+nextPosition; returns false if the buffer is full. */
    @Override
    public boolean tryStart(M message, P next) {
        boolean ok = queue.offer(new Entry<>(message, next));
        if (ok) {
            outstanding.incrementAndGet();
        }
        return ok;
    }

    /** Returns the next contiguous commit position, or null if none is ready. */
    @Override
    public P commitReadyNext() {
        return commitNext;
    }

    /** Request stop and wait for the lane to finish (or time out). */
    @Override
    public void stopAndDrain(long timeoutMs) {
        running.set(false);
        lane.shutdown();
        try {
            if (!lane.awaitTermination(Math.max(0, timeoutMs), TimeUnit.MILLISECONDS)) {
                lane.shutdownNow();
                // Best effort second wait
                lane.awaitTermination(2000, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /** Lane loop: process sequentially; advance watermark only on success. */
    private void loop() {
        try {
            while (running.get() || !queue.isEmpty()) {
                Entry<M, P> e = queue.poll(100, TimeUnit.MILLISECONDS);
                if (e == null) continue;

                try {
                    processor.process(shard, e.msg);
                    metrics.inc("processed",shard );
                    // Success => advance watermark to the next position
                    commitNext = e.next;
                } catch (Exception ex) {
                    // Failure: do NOT advance commitNext (maintains contiguous commit).
                    // Consider logging/DLQ here.
                } finally {
                    outstanding.decrementAndGet();
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /** Work item: the message plus its next commit position. */
    private static final class Entry<M, P> {
        final M msg;
        final P next;
        Entry(M msg, P next) {
            this.msg = msg;
            this.next = next;
        }
    }
}
