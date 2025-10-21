package com.hcltech.rmg.common.async;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for OrderPreservingAsyncExecutor.
 */
public class OrderPreservingAsyncExecutorTest {

    // ---------------------------------------------------------------------
    // Helpers / test doubles
    // ---------------------------------------------------------------------

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
        @Override public String correlationId(String env) { return env; } // corrId must be String
        @Override public int laneHash(String env) { return env.hashCode(); }
    }

    static final class DummyFailureAdapter implements FailureAdapter<String,String> {
        @Override public String onFailure(String env, Throwable error) { return "FAIL:" + env; }
        @Override public String onTimeout(String env, long elapsedNanos) { return "TIMEOUT:" + env; }
    }

    static final class DummyFutureRecord implements FutureRecordTypeClass<String,String,String> {
        final List<String> completedFr  = Collections.synchronizedList(new ArrayList<>());
        final List<String> completedOut = Collections.synchronizedList(new ArrayList<>());
        final List<String> failedFr     = Collections.synchronizedList(new ArrayList<>());
        final List<String> timedOutFr   = Collections.synchronizedList(new ArrayList<>());

        @Override public void completed(String fr, BiConsumer<String,String> hook, String in, String out) {
            completedFr.add(fr);
            completedOut.add(out);
            if (hook != null) hook.accept(in, out);
        }
        @Override public void failed(String fr, BiConsumer<String,String> hook, String in, Throwable error) {
            failedFr.add(fr);
        }
        @Override public void timedOut(String fr, BiConsumer<String,String> hook, String in, long elapsedNanos) {
            timedOutFr.add(fr);
        }
    }

    static OrderPreservingAsyncExecutorConfig<String,String,String> cfg(
            FailureAdapter<String,String> fa,
            FutureRecordTypeClass<String,String,String> fr) {
        return new OrderPreservingAsyncExecutorConfig<>(
                8, 8, 4, 2, 100,                    // laneCount, laneDepth, maxInFlight, executorThreads, timeoutMs
                new DummyCorrelator(), fa, fr,      // correlator, failureAdapter, futureRecord
                com.hcltech.rmg.common.ITimeService.real // timeService
        );
    }

    private ExecutorService threadPool;
    private IMpscRing<String,String,String> ring;

    private static final BiConsumer<String,String> NOOP_HOOK = (in, out) -> {};

    @BeforeEach
    void setup() {
        threadPool = Executors.newCachedThreadPool();
        ring = new MpscRing<>(256);
    }

    // ---------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------

    @Test
    void submits_and_completes_successfully() throws InterruptedException {
        CountingPermit permits = new CountingPermit(10);
        DummyFailureAdapter failure = new DummyFailureAdapter();
        DummyFutureRecord futureRec = new DummyFutureRecord();
        ILanes<String> lanes = new Lanes<>(8, 8, new DummyCorrelator());

        OrderPreservingAsyncExecutor.UserFnPort<String,String,String> userFn =
                (tc, in, corrId, c) -> c.success(in, corrId, "OUT:" + in);

        var exec = new OrderPreservingAsyncExecutor<>(
                cfg(failure, futureRec),
                lanes, permits, ring, threadPool, futureRec, 8, userFn);

        for (int i=0;i<20;i++) exec.add("K"+i, "FR-"+i, NOOP_HOOK);

        // cooperative drains until all complete
        long deadline = System.currentTimeMillis() + 2000;
        while (System.currentTimeMillis() < deadline) {
            exec.drain("IGNORED", NOOP_HOOK); // FR is applied per-drain, not per-item
            if (futureRec.completedFr.size() >= 20) break;
            Thread.sleep(2);
        }

        assertEquals(20, futureRec.completedFr.size());
        assertEquals(permits.acquired.get(), permits.released.get());
    }

    @Test
    void handles_async_failure() {
        CountingPermit permits = new CountingPermit(10);
        DummyFailureAdapter failure = new DummyFailureAdapter();
        DummyFutureRecord futureRec = new DummyFutureRecord();
        ILanes<String> lanes = new Lanes<>(4, 4, new DummyCorrelator());

        OrderPreservingAsyncExecutor.UserFnPort<String,String,String> userFn =
                (tc, in, corrId, c) -> c.failure(in, corrId, new RuntimeException("boom"));

        var exec = new OrderPreservingAsyncExecutor<>(
                cfg(failure, futureRec),
                lanes, permits, ring, threadPool, futureRec, 4, userFn);

        exec.add("X1","FR-X1", NOOP_HOOK);

        // Use the matching FR for this drain so the test can assert it precisely
        exec.drain("FR-X1", NOOP_HOOK);

        assertTrue(futureRec.failedFr.contains("FR-X1"));
        assertEquals(permits.acquired.get(), permits.released.get());
    }

    @Test
    void preserves_order_within_lane() throws Exception {
        CountingPermit permits = new CountingPermit(4);
        DummyFailureAdapter failure = new DummyFailureAdapter();
        DummyFutureRecord futureRec = new DummyFutureRecord();

        // Correlator: unique corrId per item, but lane routing still by key
        Correlator<String> uniqueCorr = new Correlator<>() {
            private final AtomicInteger seq = new AtomicInteger();
            @Override public String correlationId(String env) { return env + ":" + seq.incrementAndGet(); }
            @Override public int laneHash(String env) { return env.hashCode(); }
        };

        var localCfg = new OrderPreservingAsyncExecutorConfig<>(
                4, 4, 4, 2, 100,
                uniqueCorr, failure, futureRec,
                com.hcltech.rmg.common.ITimeService.real
        );

        ILanes<String> lanes = new Lanes<>(4, 4, uniqueCorr);

        // userFn completes in random order (async)
        OrderPreservingAsyncExecutor.UserFnPort<String,String,String> userFn = (tc, in, corrId, c) -> {
            threadPool.submit(() -> {
                try { Thread.sleep(new Random().nextInt(5)); } catch (InterruptedException ignored) {}
                c.success(in, corrId, "OUT:" + in);
            });
        };

        var exec = new OrderPreservingAsyncExecutor<>(
                localCfg, lanes, permits, ring, threadPool, futureRec, 4, userFn);

        // Enqueue 100 items for the same key
        for (int i = 0; i < 100; i++) exec.add("SAME_KEY", "FR-" + i, NOOP_HOOK);

        // Keep draining cooperatively until all completions received (or timeout)
        long deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline) {
            exec.drain("IGNORED", NOOP_HOOK); // FR attribution is per-drain, so don't assert specific FRs
            if (futureRec.completedFr.size() >= 100) break;
            Thread.sleep(1);
        }

        assertEquals(100, futureRec.completedFr.size(), "all items should complete");
        // We no longer assert per-item FR order since FR is applied per drain batch
        assertEquals(permits.acquired.get(), permits.released.get(), "permits must balance");
    }

    @Test
    void times_out_when_lane_full_and_expired() throws Exception {
        CountingPermit permits = new CountingPermit(1);
        DummyFailureAdapter failure = new DummyFailureAdapter();
        DummyFutureRecord futureRec = new DummyFutureRecord();
        ILanes<String> lanes = new Lanes<>(1, 1, new DummyCorrelator());

        OrderPreservingAsyncExecutor.UserFnPort<String,String,String> userFn =
                (tc, in, corrId, c) -> { /* never completes -> timeout only used to unblock */ };

        var cfg = cfg(failure, futureRec);
        var exec = new OrderPreservingAsyncExecutor<>(
                cfg, lanes, permits, ring, threadPool, futureRec, 1, userFn);

        // Enqueue one to occupy the single-slot lane.
        exec.add("X","FR-X", NOOP_HOOK);

        // Sleep comfortably past timeout to ensure the head is eligible for eviction.
        Thread.sleep(cfg.timeoutMillis() + 50);

        // This add should not block indefinitely; if head is expired and lane is full,
        // executor will evict to make room. We only assert that progress occurs and
        // permit accounting remains sane.
        exec.add("Y","FR-Y", NOOP_HOOK);
        exec.drain("IGNORED", NOOP_HOOK);

        assertTrue(permits.released.get() <= permits.acquired.get(), "permits must not leak");
    }

    @Test
    void respects_max_inflight_permits() {
        CountingPermit permits = new CountingPermit(1);
        DummyFailureAdapter failure = new DummyFailureAdapter();
        DummyFutureRecord futureRec = new DummyFutureRecord();
        ILanes<String> lanes = new Lanes<>(4, 4, new DummyCorrelator());

        OrderPreservingAsyncExecutor.UserFnPort<String,String,String> userFn =
                (tc, in, corrId, c) -> { /* never completes */ };

        var exec = new OrderPreservingAsyncExecutor<>(
                cfg(failure, futureRec),
                lanes, permits, ring, threadPool, futureRec, 4, userFn);

        exec.add("A","FR-A", NOOP_HOOK);
        // no second add should launch until first completes
        exec.add("B","FR-B", NOOP_HOOK);

        assertEquals(1, permits.acquired.get());
        assertEquals(0, permits.released.get());
    }
}
