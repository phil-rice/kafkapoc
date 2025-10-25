package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.common.async.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Contract tests for OrderPreservingAsyncExecutor.
 * Implementors override the factory methods to plug in local config/wiring.
 */
public abstract class AbstractEnvelopeAsyncProcessingFunctionTest {

    protected ExecutorService pool;
    protected IMpscRing<String, String, String> ring;

    protected static final BiConsumer<String,String> NOOP_HOOK = (in, out) -> {};

    @BeforeEach
    void setUp() {
        pool = Executors.newFixedThreadPool(poolThreads());
        ring = new MpscRing<>(ringCapacity());
    }

    @AfterEach
    void tearDown() {
        pool.shutdownNow();
    }

    // ---- Template hooks implementors can override as needed ----
    protected int poolThreads() { return Math.max(4, Runtime.getRuntime().availableProcessors()); }
    protected int ringCapacity() { return 4096; }
    protected int laneCount() { return 64; }
    protected int laneDepth() { return 8; }
    protected int maxInFlight() { return 64; }
    protected int timeoutMs() { return 200; }

    protected Correlator<String> correlator() {
        return new Correlator<>() {
            @Override public String correlationId(String env) { return env; }
            @Override public int laneHash(String env) { return env.hashCode(); }
        };
    }

    protected FailureAdapter<String,String> failureAdapter() {
        return new FailureAdapter<>() {
            @Override public String onFailure(String env, Throwable error) { return "FAIL:"+env; }
            @Override public String onTimeout(String env, long elapsedNanos) { return "TIMEOUT:"+env; }
        };
    }

    protected FutureRecordTypeClass<String,String,String> futureRecord(List<String> completedFr,
                                                                       List<String> completedOut,
                                                                       List<String> failedFr) {
        return new FutureRecordTypeClass<>() {
            @Override public void completed(String fr, BiConsumer<String, String> hook, String in, String out) {
                completedFr.add(fr);
                completedOut.add(out);
                if (hook != null) hook.accept(in, out);
            }
            @Override public void failed(String fr, BiConsumer<String, String> hook, String in, Throwable error) {
                failedFr.add(fr);
            }
            @Override public void timedOut(String fr, BiConsumer<String, String> hook, String in, long elapsedNanos) {
                // timeout not used in these unit contract tests
            }
        };
    }

    protected OrderPreservingAsyncExecutorConfig<String,String,String> config(FutureRecordTypeClass<String,String,String> frTc) {
        return new OrderPreservingAsyncExecutorConfig<>(
                laneCount(), laneDepth(), maxInFlight(), poolThreads(), timeoutMs(),
                correlator(), failureAdapter(), frTc, com.hcltech.rmg.common.ITimeService.real
        );
    }

    protected PermitManager permits() { return new AtomicPermitManager(maxInFlight()); }

    protected ILanes<String> lanes() { return new Lanes<>(laneCount(), laneDepth(), correlator()); }

    /** Factory so implementors can swap wiring if needed (should return your production ctor). */
    protected OrderPreservingAsyncExecutor<String,String,String> newExecutor(
            OrderPreservingAsyncExecutorConfig<String,String,String> cfg,
            ILanes<String> lanes,
            PermitManager permits,
            IMpscRing<String,String,String> ring,
            ExecutorService pool,
            FutureRecordTypeClass<String,String,String> frTc,
            int laneCount,
            OrderPreservingAsyncExecutor.UserFnPort<String,String,String> userFn
    ) {
        return new OrderPreservingAsyncExecutor<>(cfg, lanes, permits, ring, pool, frTc, laneCount, userFn);
    }

    // =====================================================================================
    // Tests
    // =====================================================================================

    @Test
    void spans_multiple_threads() throws Exception {
        var completedFr = Collections.synchronizedList(new ArrayList<String>());
        var completedOut = Collections.synchronizedList(new ArrayList<String>());
        var failedFr = Collections.synchronizedList(new ArrayList<String>());
        var frTc = futureRecord(completedFr, completedOut, failedFr);

        // Capture thread names where user work runs (should be pool threads)
        var seenThreads = ConcurrentHashMap.newKeySet();

        OrderPreservingAsyncExecutor.UserFnPort<String,String,String> userFn = (tc, in, corr, c) -> {
            seenThreads.add(Thread.currentThread().getName());
            // cheap work
            c.success(in, corr, "OUT:"+in);
        };

        var exec = newExecutor(config(frTc), lanes(), permits(), ring, pool, frTc, laneCount(), userFn);

        int total = 200;
        for (int i = 0; i < total; i++) exec.add("K"+i, "FR-"+i, NOOP_HOOK);

        long endBy = System.currentTimeMillis() + 3_000;
        while (completedFr.size() < total && System.currentTimeMillis() < endBy) {
            exec.drain("IGNORED", NOOP_HOOK);
            Thread.sleep(1);
        }

        assertEquals(total, completedFr.size(), "all items should complete");
        // If pool > 1 and inflight > 1, expect >1 distinct threads
        assertTrue(seenThreads.size() >= 2, "work should span multiple pool threads (saw: " + seenThreads + ")");
    }

    @Test
    void preserves_order_within_same_key() throws Exception {
        var completedFr  = Collections.synchronizedList(new ArrayList<String>());
        var completedOut = Collections.synchronizedList(new ArrayList<String>());
        var failedFr     = Collections.synchronizedList(new ArrayList<String>());

        var frTc = futureRecord(completedFr, completedOut, failedFr);

        // Route by prefix before '|', so K|0, K|1, ... all go to the same lane
        Correlator<String> sameLaneCorr = new Correlator<>() {
            @Override public String correlationId(String env) { return env; }
            @Override public int laneHash(String env) {
                int p = env.indexOf('|');
                String key = (p >= 0) ? env.substring(0, p) : env;
                return key.hashCode();
            }
        };

        var localCfg = new OrderPreservingAsyncExecutorConfig<>(
                laneCount(), laneDepth(), maxInFlight(), poolThreads(), timeoutMs(),
                sameLaneCorr, failureAdapter(), frTc, com.hcltech.rmg.common.ITimeService.real
        );

        ILanes<String> localLanes = new Lanes<>(laneCount(), laneDepth(), sameLaneCorr);
        PermitManager localPermits = permits();

        // Simulate arbitrary async time on the pool
        OrderPreservingAsyncExecutor.UserFnPort<String,String,String> userFn = (tc, in, corr, c) -> {
            try { Thread.sleep(ThreadLocalRandom.current().nextInt(2)); } catch (InterruptedException ignored) {}
            c.success(in, corr, in); // echo input for order check
        };

        var exec = newExecutor(localCfg, localLanes, localPermits, ring, pool, frTc, laneCount(), userFn);

        int N = 200;
        for (int i = 0; i < N; i++) {
            exec.add("K|" + i, "FR-" + i, NOOP_HOOK); // SAME lane (prefix K), sequence in suffix
        }

        long endBy = System.currentTimeMillis() + 5_000;
        while (completedOut.size() < N && System.currentTimeMillis() < endBy) {
            exec.drain("IGNORED", NOOP_HOOK);
            Thread.sleep(1);
        }
        assertEquals(N, completedOut.size(), "all items should complete");

        // Monotone order check by suffix after '|'
        int prev = -1;
        for (String val : completedOut) {
            int bar = val.indexOf('|');
            assertTrue(bar >= 0, "expected '|' in value: " + val);
            int cur = Integer.parseInt(val.substring(bar + 1));
            assertTrue(cur > prev, "order must be preserved: " + prev + " -> " + cur + " (" + val + ")");
            prev = cur;
        }
    }


    @Test
    void hook_is_called_for_each_completion() throws Exception {
        var completedFr = Collections.synchronizedList(new ArrayList<String>());
        var completedOut = Collections.synchronizedList(new ArrayList<String>());
        var failedFr = Collections.synchronizedList(new ArrayList<String>());
        var frTc = futureRecord(completedFr, completedOut, failedFr);

        var hookCount = new AtomicInteger();
        var lastHookIn = new AtomicReference<String>();
        var lastHookOut = new AtomicReference<String>();

        BiConsumer<String,String> hook = (in, out) -> {
            hookCount.incrementAndGet();
            lastHookIn.set(in);
            lastHookOut.set(out);
        };

        OrderPreservingAsyncExecutor.UserFnPort<String,String,String> userFn = (tc, in, corr, c) -> c.success(in, corr, "OUT:"+in);

        var exec = newExecutor(config(frTc), lanes(), permits(), ring, pool, frTc, laneCount(), userFn);

        int total = 50;
        for (int i = 0; i < total; i++) exec.add("K"+i, "FR-"+i, hook);

        long endBy = System.currentTimeMillis() + 2_000;
        while (completedFr.size() < total && System.currentTimeMillis() < endBy) {
            exec.drain("IGNORED", hook); // FR/hook applied per drain call
            Thread.sleep(1);
        }

        assertEquals(total, completedFr.size(), "all items should complete");
        assertTrue(hookCount.get() > 0, "hook should be called");
        assertNotNull(lastHookIn.get(), "hook in captured");
        assertNotNull(lastHookOut.get(), "hook out captured");
    }

    @Test
    void diagnostics_snapshot_is_sane() {
        var completedFr = Collections.synchronizedList(new ArrayList<String>());
        var completedOut = Collections.synchronizedList(new ArrayList<String>());
        var failedFr = Collections.synchronizedList(new ArrayList<String>());
        var frTc = futureRecord(completedFr, completedOut, failedFr);

        OrderPreservingAsyncExecutor.UserFnPort<String,String,String> userFn =
                (tc, in, corr, c) -> c.success(in, corr, "OUT:"+in);

        var exec = newExecutor(config(frTc), lanes(), permits(), ring, pool, frTc, laneCount(), userFn);

        for (int i = 0; i < 100; i++) exec.add("K"+i, "FR-"+i, NOOP_HOOK);

        // Before draining, some lanes should be in use; after draining, many should be empty.
        LaneDiagnostics d1 = exec.laneDiagnostics();
        exec.drain("IGNORED", NOOP_HOOK);
        LaneDiagnostics d2 = exec.laneDiagnostics();

        assertEquals(laneCount(), d1.laneCount);
        assertEquals(laneCount(), d2.laneCount);
        assertTrue(d2.empty >= d1.empty, "after drain, empty must not decrease");
    }
    @Test
    void respects_max_inflight_permits() {
        var completedFr  = Collections.synchronizedList(new ArrayList<String>());
        var completedOut = Collections.synchronizedList(new ArrayList<String>());
        var failedFr     = Collections.synchronizedList(new ArrayList<String>());
        var frTc = futureRecord(completedFr, completedOut, failedFr);

        // single permit
        PermitManager single = new AtomicPermitManager(1);

        // never completes -> keeps the permit
        OrderPreservingAsyncExecutor.UserFnPort<String,String,String> userFn = (tc, in, corr, c) -> { /* no completion */ };

        var exec = newExecutor(config(frTc), lanes(), single, ring, pool, frTc, laneCount(), userFn);

        exec.add("A","FR-A", NOOP_HOOK);
        exec.add("B","FR-B", NOOP_HOOK);

        // one held -> available should be 0
        assertEquals(0, single.availablePermits(), "one permit should be held (available==0)");
    }


    @Test
    void async_failure_is_reported_and_permit_released() throws Exception {
        var completedFr = Collections.synchronizedList(new ArrayList<String>());
        var failedFr    = Collections.synchronizedList(new ArrayList<String>());
        var frTc = futureRecord(completedFr, Collections.synchronizedList(new ArrayList<>()), failedFr);

        var permits = new AtomicPermitManager(4);

        OrderPreservingAsyncExecutor.UserFnPort<String,String,String> userFn =
                (tc, in, corr, c) -> c.failure(in, corr, new RuntimeException("boom"));

        var exec = newExecutor(config(frTc), lanes(), permits, ring, pool, frTc, laneCount(), userFn);

        exec.add("X","FR-X", NOOP_HOOK);

        long endBy = System.currentTimeMillis() + 1000;
        while (failedFr.isEmpty() && System.currentTimeMillis() < endBy) {
            exec.drain("FR-X", NOOP_HOOK);
            Thread.sleep(1);
        }

        assertTrue(failedFr.contains("FR-X"), "failure should be reported");
        assertEquals(permits.maxPermits(), permits.availablePermits(), "permit should be released");
    }
    @Test
    void executor_rejection_becomes_failure() throws Exception {
        var completedFr = Collections.synchronizedList(new ArrayList<String>());
        var failedFr    = Collections.synchronizedList(new ArrayList<String>());
        var frTc = futureRecord(completedFr, Collections.synchronizedList(new ArrayList<>()), failedFr);

        // ExecutorService that immediately rejects all tasks
        ExecutorService rejecting = new AbstractExecutorService() {
            @Override public void execute(Runnable command) { throw new RejectedExecutionException("reject"); }
            @Override public void shutdown() {}
            @Override public List<Runnable> shutdownNow() { return List.of(); }
            @Override public boolean isShutdown() { return false; }
            @Override public boolean isTerminated() { return false; }
            @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return true; }
        };

        try {
            var exec = newExecutor(config(frTc), lanes(), permits(), ring, rejecting, frTc, laneCount(),
                    (tc, in, corr, c) -> c.success(in, corr, "OUT:" + in));

            exec.add("R", "FR-R", NOOP_HOOK);

            long endBy = System.currentTimeMillis() + 1_000;
            while (failedFr.isEmpty() && System.currentTimeMillis() < endBy) {
                exec.drain("FR-R", NOOP_HOOK);
                Thread.sleep(1);
            }

            assertTrue(failedFr.contains("FR-R"), "rejection should surface as failure");
        } finally {
            rejecting.shutdownNow();
        }
    }


    @Test
    void drain_context_is_per_call_not_sticky() throws Exception {
        var completedFr  = Collections.synchronizedList(new ArrayList<String>());
        var completedOut = Collections.synchronizedList(new ArrayList<String>());
        var failedFr     = Collections.synchronizedList(new ArrayList<String>());
        var frTc = futureRecord(completedFr, completedOut, failedFr);

        // Synchronous: executor offloads, but work completes immediately
        OrderPreservingAsyncExecutor.UserFnPort<String,String,String> userFn =
                (tc, in, corr, c) -> c.success(in, corr, "OUT:"+in);

        var exec = newExecutor(config(frTc), lanes(), permits(), ring, pool, frTc, laneCount(), userFn);

        // A completes and is drained with FR-1
        exec.add("A","FR-A", NOOP_HOOK);
        long end1 = System.currentTimeMillis() + 1000;
        while (!completedFr.contains("FR-1") && System.currentTimeMillis() < end1) {
            exec.drain("FR-1", NOOP_HOOK);
            Thread.sleep(1);
        }
        assertTrue(completedFr.contains("FR-1"), "A should be attributed to first drain FR");

        // B completes and is drained with FR-2
        exec.add("B","FR-B", NOOP_HOOK);
        long end2 = System.currentTimeMillis() + 1000;
        while (!completedFr.contains("FR-2") && System.currentTimeMillis() < end2) {
            exec.drain("FR-2", NOOP_HOOK);
            Thread.sleep(1);
        }
        assertTrue(completedFr.contains("FR-2"), "B should be attributed to second drain FR");
    }



}
