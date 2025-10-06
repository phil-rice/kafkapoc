package com.hcltech.rmg.cepstate.retry;

import com.hcltech.rmg.cepstate.metrics.IMetrics;
import com.hcltech.rmg.common.ITimeService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BucketRunnerTest {

    // --- Fakes ---

    static final class FakeTime implements ITimeService {
        private volatile long now;
        FakeTime(long startMs) { this.now = startMs; }
        @Override public long currentTimeMillis() { return now; }
        void set(long t) { this.now = t; }
        void advance(long d) { this.now += d; }
    }

    /** Minimal IRetryBuckets that returns a one-shot list then empties. */
    static final class FakeBuckets implements IRetryBuckets {
        private final Object lock = new Object();
        private List<RetryKey> next = List.of();
        private final AtomicInteger drains = new AtomicInteger(0);

        void setNext(List<RetryKey> keys) {
            synchronized (lock) { next = new ArrayList<>(keys); }
        }
        int drainCalls() { return drains.get(); }

        @Override public boolean addToRetryBucket(String topic, String domainId, long offset, long leaseAcquireTime, int retriesSoFar) {
            throw new UnsupportedOperationException("not used in these tests");
        }

        @Override public List<RetryKey> retryKeysForNow() {
            drains.incrementAndGet();
            synchronized (lock) {
                List<RetryKey> out = next;
                next = List.of(); // one-shot drain
                return out;
            }
        }
    }

    // Shared per-test resources
    private ScheduledExecutorService scheduler;

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    @Test
    void schedulesAtExactDueTimes_andInvokesHandler() throws Exception {
        // Arrange
        FakeTime time = new FakeTime(10_000);
        FakeBuckets buckets = new FakeBuckets();

        // 3 items: past, short, longer (order preserved by fake buckets)
        var k1 = new RetryKey("T","A",  9_990); // past -> delay 0
        var k2 = new RetryKey("T","B", 10_020); // 20 ms
        var k3 = new RetryKey("T","C", 10_050); // 50 ms
        buckets.setNext(List.of(k1, k2, k3));

        var counters = new ConcurrentHashMap<String, LongAdder>();
        IMetrics metrics = IMetrics.memoryMetrics(counters);

        var seenDelays = new CopyOnWriteArrayList<Long>();
        CountDownLatch handled = new CountDownLatch(3);

        scheduler = Executors.newSingleThreadScheduledExecutor();

        BucketRunner runner = new BucketRunner(
                buckets,
                /*tickMillis*/ 10,
                scheduler,
                time,
                (key, delay) -> seenDelays.add(delay),
                metrics,
                key -> { handled.countDown(); return CompletableFuture.completedFuture(null); }
        );

        // Act
        runner.start();

        // Assert callback delays quickly
        awaitCount(seenDelays, 3, 300);
        // We expect [0, 20, 50] (minor timing jitter may produce tiny >0 for the "0" item)
        var sorted = new ArrayList<>(seenDelays);
        sorted.sort(Long::compare);
        assertEquals(List.of(0L, 20L, 50L), sorted, "delays should honor dueAtMs");

        // Assert handler fired for each scheduled task
        assertTrue(handled.await(500, TimeUnit.MILLISECONDS), "all handlers should run");

        // Metrics
        assertEquals(1L, getCount(counters, "retryBucketRunner.started"));
        assertEquals(3L, getCount(counters, "retryBucketRunner.scheduled"));

        runner.close();
        assertEquals(1L, getCount(counters, "retryBucketRunner.stopped"));
    }

    @Test
    void stopPreventsFurtherDrains() throws Exception {
        FakeTime time = new FakeTime(0);
        FakeBuckets buckets = new FakeBuckets();
        var counters = new ConcurrentHashMap<String, LongAdder>();
        IMetrics metrics = IMetrics.memoryMetrics(counters);

        var seenDelays = new CopyOnWriteArrayList<Long>();
        CountDownLatch firstHandled = new CountDownLatch(1);

        scheduler = Executors.newSingleThreadScheduledExecutor();

        BucketRunner runner = new BucketRunner(
                buckets, 10, scheduler, time,
                (k,d) -> seenDelays.add(d),
                metrics,
                k -> { firstHandled.countDown(); return CompletableFuture.completedFuture(null); }
        );

        // First batch
        buckets.setNext(List.of(new RetryKey("T","A", 5)));
        runner.start();

        assertTrue(firstHandled.await(300, TimeUnit.MILLISECONDS));
        runner.close();
        long scheduledSoFar = getCount(counters, "retryBucketRunner.scheduled");

        // Second batch after stop — should not be picked
        buckets.setNext(List.of(new RetryKey("T","B", 5)));
        Thread.sleep(200);

        assertEquals(scheduledSoFar, getCount(counters, "retryBucketRunner.scheduled"),
                "no additional schedules after close()");
    }

    // --- small helpers ---

    private static long getCount(ConcurrentHashMap<String, LongAdder> counters, String name) {
        LongAdder a = counters.get(name);
        return a == null ? 0L : a.sum();
    }

    private static void awaitCount(List<?> list, int count, long timeoutMs) throws InterruptedException, TimeoutException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (list.size() >= count) return;
            Thread.sleep(5);
        }
        throw new TimeoutException("timed out waiting for size " + count + " (have " + list.size() + ")");
    }
}
