package com.hcltech.rmg.cepstate.metrics;

import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.*;

class IMetricsTest {

    @Test
    void nullMetrics_doesNotThrow() {
        IMetrics metrics = IMetrics.nullMetrics();
        assertDoesNotThrow(() -> metrics.increment("anything"));
    }

    @Test
    void memoryMetrics_writesIntoSuppliedMap() {
        ConcurrentHashMap<String, LongAdder> counters = new ConcurrentHashMap<>();
        IMetrics metrics = IMetrics.memoryMetrics(counters);

        metrics.increment("requests");

        assertTrue(counters.containsKey("requests"));
        assertEquals(1L, counters.get("requests").sum());
    }

    @Test
    void memoryMetrics_incrementsSameMetricMultipleTimes() {
        ConcurrentHashMap<String, LongAdder> counters = new ConcurrentHashMap<>();
        IMetrics metrics = IMetrics.memoryMetrics(counters);

        metrics.increment("requests");
        metrics.increment("requests");
        metrics.increment("requests");

        assertEquals(3L, counters.get("requests").sum());
        assertEquals(1, counters.size(), "Should only create one counter entry");
    }

    @Test
    void memoryMetrics_keepsSeparateCountersPerName() {
        ConcurrentHashMap<String, LongAdder> counters = new ConcurrentHashMap<>();
        IMetrics metrics = IMetrics.memoryMetrics(counters);

        metrics.increment("a");
        metrics.increment("b");
        metrics.increment("b");

        assertEquals(1L, counters.get("a").sum());
        assertEquals(2L, counters.get("b").sum());
        assertEquals(2, counters.size());
    }

    @Test
    void memoryMetrics_nullNameThrows() {
        ConcurrentHashMap<String, LongAdder> counters = new ConcurrentHashMap<>();
        IMetrics metrics = IMetrics.memoryMetrics(counters);

        assertThrows(NullPointerException.class, () -> metrics.increment(null));
        assertTrue(counters.isEmpty(), "No counters should be created on failure");
    }

    @Test
    void memoryMetrics_isThreadSafeUnderContention() throws InterruptedException {
        ConcurrentHashMap<String, LongAdder> counters = new ConcurrentHashMap<>();
        IMetrics metrics = IMetrics.memoryMetrics(counters);

        final int threads = 8;
        final int iterationsPerThread = 10_000;
        final String name = "concurrent";

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            pool.execute(() -> {
                try {
                    start.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        metrics.increment(name);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "Workers did not finish in time");
        pool.shutdownNow();

        long expected = (long) threads * iterationsPerThread;
        assertEquals(expected, counters.get(name).sum());
    }
}
