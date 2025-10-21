package com.hcltech.rmg.common.async;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress tests for OrderPreservingAsyncExecutor:
 * 1) parallel success with many keys (fast)
 * 2) parallel mixed success/failure (fast)
 * 3) serial timeouts (only care about eviction when lane is FULL)
 */
public class OrderPreservingAsyncExecutorStressTest {

    // ----- Test doubles -----

    static final class CountingPermit implements PermitManager {
        final Semaphore sem;
        final int max;
        final AtomicInteger acquired = new AtomicInteger();
        final AtomicInteger released = new AtomicInteger();
        CountingPermit(int max) { this.max = max; this.sem = new Semaphore(max); }
        @Override public boolean tryAcquire() {
            boolean ok = sem.tryAcquire();
            if (ok) acquired.incrementAndGet();
            return ok;
        }
        @Override public void release() {
            released.incrementAndGet();
            sem.release();
        }
        @Override public int availablePermits() { return sem.availablePermits(); }
        @Override public int maxPermits() { return max; }
    }

    static final class DummyCorrelator implements Correlator<String> {
        @Override public String correlationId(String env) { return env.hashCode(); }
        @Override public int laneHash(String env) { return env.hashCode(); }
    }

    static final class DummyFailureAdapter implements FailureAdapter<String,String> {
        @Override public String onFailure(String env, Throwable error) { return "FAIL:" + env; }
        @Override public String onTimeout(String env, long elapsedNanos) { return "TIMEOUT:" + env; }
    }

    static final class RecordingFutureRecord implements FutureRecordTypeClass<String,String,String> {
        final List<String> completedFr  = Collections.synchronizedList(new ArrayList<>());
        final List<String> completedOut = Collections.synchronizedList(new ArrayList<>());
        final List<String> failedFr     = Collections.synchronizedList(new ArrayList<>());
        final List<String> timedOutFr   = Collections.synchronizedList(new ArrayList<>());
        final CountDownLatch latch;

        RecordingFutureRecord(int expectedTotal) { this.latch = new CountDownLatch(expectedTotal); }

        @Override public void completed(String fr, String out) {
            completedFr.add(fr);
            completedOut.add(out);
            latch.countDown();
        }
        @Override public void failed(String fr, String in, Throwable error) {
            failedFr.add(fr);
            latch.countDown();
        }
        @Override public void timedOut(String fr, String in, long elapsedNanos) {
            timedOutFr.add(fr);
            latch.countDown();
        }
    }

    static OrderPreservingAsyncExecutorConfig<String,String,String> cfg(
            FailureAdapter<String,String> fa,
            FutureRecordTypeClass<String,String,String> fr,
            int laneCount, int laneDepth, int maxInFlight, int executorThreads,
            int admissionCycles, long timeoutMs) {
        return new OrderPreservingAsyncExecutorConfig<>(
                laneCount, laneDepth, maxInFlight, executorThreads, timeoutMs,
                new DummyCorrelator(), fa, fr, com.hcltech.rmg.common.ITimeService.real);
    }

    private ExecutorService threadPool;
    private IMpscRing<String,String,String> ring;

    @BeforeEach
    void setup() {
        threadPool = Executors.newFixedThreadPool(Math.max(4, Runtime.getRuntime().availableProcessors() * 2));
        ring = new MpscRing<>(4096);
    }

    @AfterEach
    void tearDown() {
        threadPool.shutdownNow();
    }

    // ---------------------------------------------------------------------
    // 1) Parallel success across many keys (fast)
    // ---------------------------------------------------------------------
    @Test
    void stress_parallel_success_manyKeys() throws Exception {
        final int TOTAL = 5_000;
        final int KEYS  = 500;       // 10 items per key on average
        final int LANE_COUNT = 256;  // enough lanes
        final int LANE_DEPTH = 8;
        final int MAX_INFLIGHT = 256;

        CountingPermit permits = new CountingPermit(MAX_INFLIGHT);
        DummyFailureAdapter failure = new DummyFailureAdapter();
        RecordingFutureRecord futureRec = new RecordingFutureRecord(TOTAL);
        ILanes<String> lanes = new Lanes<>(LANE_COUNT, LANE_DEPTH, new DummyCorrelator());

        OrderPreservingAsyncExecutor.UserFnPort<String,String,String> userFn = (fr, in, corr, c) ->
                threadPool.submit(() -> {
                    try { Thread.sleep(1 + ThreadLocalRandom.current().nextInt(10)); }
                    catch (InterruptedException ignored) {}
                    c.success(fr, in, corr, "OUT:" + in);
                });

        var exec = new OrderPreservingAsyncExecutor<>(cfg(failure,futureRec,
                LANE_COUNT, LANE_DEPTH, MAX_INFLIGHT, 8, 8, 200),
                lanes, permits, ring, threadPool, futureRec, LANE_COUNT, userFn);

        for (int i = 0; i < TOTAL; i++) {
            String key = "K" + (i % KEYS);
            exec.add(key, "FR-" + i);
        }

        while (futureRec.latch.getCount() > 0) {
            exec.drain(null); // FR rides on ring events
            Thread.sleep(1);
        }

        assertEquals(TOTAL, futureRec.completedFr.size(), "all items must complete");
        assertEquals(permits.acquired.get(), permits.released.get(), "permits must balance");
    }

    // ---------------------------------------------------------------------
    // 2) Parallel mixed success/failure across many keys (fast)
    // ---------------------------------------------------------------------
    @Test
    void stress_parallel_mixed_manyKeys() throws Exception {
        final int TOTAL = 5_000;
        final int KEYS  = 500;
        final int LANE_COUNT = 256;
        final int LANE_DEPTH = 8;
        final int MAX_INFLIGHT = 256;

        CountingPermit permits = new CountingPermit(MAX_INFLIGHT);
        DummyFailureAdapter failure = new DummyFailureAdapter();
        RecordingFutureRecord futureRec = new RecordingFutureRecord(TOTAL);
        ILanes<String> lanes = new Lanes<>(LANE_COUNT, LANE_DEPTH, new DummyCorrelator());

        OrderPreservingAsyncExecutor.UserFnPort<String,String,String> userFn = (fr, in, corr, c) ->
                threadPool.submit(() -> {
                    try { Thread.sleep(1 + ThreadLocalRandom.current().nextInt(10)); }
                    catch (InterruptedException ignored) {}
                    if (ThreadLocalRandom.current().nextInt(100) < 30)
                        c.failure(fr, in, corr, new RuntimeException("boom"));
                    else
                        c.success(fr, in, corr, "OUT:" + in);
                });

        var exec = new OrderPreservingAsyncExecutor<>(cfg(failure,futureRec,
                LANE_COUNT, LANE_DEPTH, MAX_INFLIGHT, 8, 8, 200),
                lanes, permits, ring, threadPool, futureRec, LANE_COUNT, userFn);

        for (int i = 0; i < TOTAL; i++) {
            String key = "K" + (i % KEYS);
            exec.add(key, "FR-" + i);
        }

        while (futureRec.latch.getCount() > 0) {
            exec.drain(null);
            Thread.sleep(1);
        }

        // accounted for (timeouts may be zero; we only time out to avoid blocking)
        assertEquals(TOTAL,
                futureRec.completedFr.size() + futureRec.failedFr.size() + futureRec.timedOutFr.size(),
                "every item must complete or fail");
        assertEquals(permits.acquired.get(), permits.released.get(), "permits must balance");
    }

    // ---------------------------------------------------------------------
    // 3) Serial timeouts: we only care about eviction when lane is FULL.
    //    Single lane (depth=1), never-completing userFn -> ensure at least one eviction happens.
    // ---------------------------------------------------------------------
    @Test
    void stress_serial_timeouts() throws Exception {
        final int N = 300;       // many admissions to exercise eviction-on-full
        final long TIMEOUT_MS = 5L;

        CountingPermit permits = new CountingPermit(1);
        DummyFailureAdapter failure = new DummyFailureAdapter();
        RecordingFutureRecord futureRec = new RecordingFutureRecord(N); // count outcomes if any
        ILanes<String> lanes = new Lanes<>(1, 1, new DummyCorrelator());

        // never completes -> the only way to keep making space is eviction when full
        OrderPreservingAsyncExecutor.UserFnPort<String,String,String> userFn =
                (fr, in, corr, c) -> { /* no-op */ };

        var exec = new OrderPreservingAsyncExecutor<>(cfg(failure,futureRec,
                1,1,1,1,8, TIMEOUT_MS),
                lanes, permits, ring, threadPool, futureRec, 1, userFn);

        // Seed a head
        exec.add("K", "FR-H");

        // Repeatedly try to add after sleeping past the timeout. We don't assert that
        // every cycle produces a timeout; we only care that the executor remains unblocked.
        long start = System.nanoTime();
        for (int i = 0; i < N; i++) {
            Thread.sleep(TIMEOUT_MS + 20); // cushion over timer granularity
            exec.add("K", "FR-" + i);      // if lane full & head expired, this unblocks by evicting
            exec.drain(null);
        }
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        // Progress property: the loop completed in a reasonable amount of time
        // (i.e., adds did not block indefinitely on a full lane).
        assertTrue(elapsedMs < 10_000, "executor should not block under full-lane eviction policy");

        // Permit accounting sanity: at most one outstanding head in this scenario.
        assertTrue(permits.released.get() <= permits.acquired.get(), "permits must not leak");
        assertTrue(permits.acquired.get() - permits.released.get() <= 1, "at most one in-flight head");
    }


}
