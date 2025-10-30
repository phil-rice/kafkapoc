package com.hcltech.rmg.common.async;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

/** Plain input env – no inheritance, no framework coupling */
class TestSeqEnv {
    private long seq;
    final String value;
    TestSeqEnv(String value) { this.value = value; }
    long getSeqField() { return seq; }
    void setSeqField(long s) { this.seq = s; }
    @Override public String toString() { return "TestSeqEnv{seq=" + seq + ", value=" + value + "}"; }
}

/** Plain output env – no inheritance, no framework coupling */
class TestOut {
    private long seq;
    final String value;
    TestOut(String value) { this.value = value; }
    long getSeqField() { return seq; }
    void setSeqField(long s) { this.seq = s; }
    @Override public String toString() { return "TestOut{seq=" + seq + ", value=" + value + "}"; }
}

/** Type classes for sequence tagging */
final class SeqTC {
    static final HasSeq<TestSeqEnv> IN_SEQ = new HasSeq<>() {
        @Override public long get(TestSeqEnv v) { return v.getSeqField(); }
        @Override public TestSeqEnv set(TestSeqEnv v, long seq) { v.setSeqField(seq); return v; }
    };
    static final HasSeq<TestOut> OUT_SEQ = new HasSeq<>() {
        @Override public long get(TestOut v) { return v.getSeqField(); }
        @Override public TestOut set(TestOut v, long seq) { v.setSeqField(seq); return v; }
    };
}

/** Simple counting PermitManager backed by a Semaphore */
class CountingPermitManager implements PermitManager {
    private final Semaphore sem;
    private final int max;
    final AtomicInteger acquired = new AtomicInteger(0);
    final AtomicInteger released = new AtomicInteger(0);
    CountingPermitManager(int max) { this.sem = new Semaphore(max); this.max = max; }
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

/** Subclass to capture ordered outputs */
class CapturingExecutor extends DefaultPerKeyAsyncExecutor<TestSeqEnv, TestOut> {
    final List<TestOut> ordered = new ArrayList<>();
    CapturingExecutor(int K,
                      Executor exec,
                      Function<TestSeqEnv, CompletionStage<TestOut>> fn,
                      FailureAdapter<TestSeqEnv, TestOut> failure,
                      PermitManager pm) {
        super(K, exec, fn, failure, pm, SeqTC.IN_SEQ, SeqTC.OUT_SEQ);
    }
    @Override protected void handleOrdered(TestOut out) {
        ordered.add(out);
    }
}

public class DefaultPerKeyAsyncExecutorTest {

    // ---------- FailureAdapter helpers (new interface) ----------

    /** Map failure/timeout to "prefix + in.value". */
    private static FailureAdapter<TestSeqEnv, TestOut> failureAdapterWithPrefix(String prefix) {
        return new FailureAdapter<>() {
            @Override public TestOut onFailure(TestSeqEnv in, Throwable error) {
                return new TestOut(prefix + in.value);
            }
            @Override public TestOut onTimeout(TestSeqEnv in, long elapsedNanos) {
                return new TestOut(prefix + in.value);
            }
        };
    }

    /** Map failure/timeout to a constant value, ignoring payload. */
    private static FailureAdapter<TestSeqEnv, TestOut> failureAdapterConst(String constant) {
        return new FailureAdapter<>() {
            @Override public TestOut onFailure(TestSeqEnv in, Throwable error) {
                return new TestOut(constant);
            }
            @Override public TestOut onTimeout(TestSeqEnv in, long elapsedNanos) {
                return new TestOut(constant);
            }
        };
    }

    @Test
    void success_inOrder_delivery_and_needsWake_contract() {
        CountingPermitManager pm = new CountingPermitManager(10);
        Executor direct = Runnable::run;

        Function<TestSeqEnv, CompletionStage<TestOut>> asyncFn =
                in -> CompletableFuture.completedFuture(new TestOut("ok:" + in.value));

        FailureAdapter<TestSeqEnv, TestOut> failure = failureAdapterWithPrefix("err:");

        CapturingExecutor exec = new CapturingExecutor(4, direct, asyncFn, failure, pm);

        // Launch three inputs
        assertEquals(AcceptResult.LAUNCHED, exec.execute(new TestSeqEnv("a")));
        assertEquals(AcceptResult.LAUNCHED, exec.execute(new TestSeqEnv("b")));
        assertEquals(AcceptResult.LAUNCHED, exec.execute(new TestSeqEnv("c")));

        // Completions enqueued; needsWake is true until we drain
        assertTrue(exec.needsWake());

        exec.onWake(); // drain all in order

        assertFalse(exec.needsWake());
        assertEquals(List.of("ok:a", "ok:b", "ok:c"),
                exec.ordered.stream().map(o -> o.value).toList());

        // Permits paired: acquired == released
        assertEquals(pm.acquired.get(), pm.released.get());
        assertEquals(pm.maxPermits(), pm.availablePermits());
    }

    @Test
    void outOfOrder_completions_still_drained_inOrder() {
        CountingPermitManager pm = new CountingPermitManager(10);
        Executor direct = Runnable::run;

        CompletableFuture<TestOut> f0 = new CompletableFuture<>();
        CompletableFuture<TestOut> f1 = new CompletableFuture<>();

        AtomicInteger counter = new AtomicInteger(0);
        Function<TestSeqEnv, CompletionStage<TestOut>> asyncFn = in -> {
            int idx = counter.getAndIncrement();
            return idx == 0 ? f0 : f1;
        };

        FailureAdapter<TestSeqEnv, TestOut> failure = failureAdapterWithPrefix("err:");

        CapturingExecutor exec = new CapturingExecutor(4, direct, asyncFn, failure, pm);

        // Launch two inputs
        assertEquals(AcceptResult.LAUNCHED, exec.execute(new TestSeqEnv("x")));
        assertEquals(AcceptResult.LAUNCHED, exec.execute(new TestSeqEnv("y")));

        // Complete second first (out-of-order)
        f1.complete(new TestOut("ok:y"));
        exec.onWake(); // nothing should drain yet (waiting for seq0)
        assertTrue(exec.ordered.isEmpty());

        // Now complete first
        f0.complete(new TestOut("ok:x"));
        exec.onWake(); // should drain x then y
        assertEquals(List.of("ok:x", "ok:y"),
                exec.ordered.stream().map(o -> o.value).toList());
    }

    @Test
    void perKeyCap_K_blocks_when_inFlight_reaches_K() {
        CountingPermitManager pm = new CountingPermitManager(10);
        Executor direct = Runnable::run;

        CompletableFuture<TestOut> f = new CompletableFuture<>();
        Function<TestSeqEnv, CompletionStage<TestOut>> asyncFn = in -> f;

        FailureAdapter<TestSeqEnv, TestOut> failure = failureAdapterWithPrefix("err:");

        CapturingExecutor exec = new CapturingExecutor(1, direct, asyncFn, failure, pm);

        assertEquals(AcceptResult.LAUNCHED, exec.execute(new TestSeqEnv("one")));
        assertEquals(AcceptResult.REJECTED_FULL, exec.execute(new TestSeqEnv("two"))); // K saturated

        // Complete first; drain; capacity should free
        f.complete(new TestOut("ok:one"));
        exec.onWake();
        assertEquals(List.of("ok:one"),
                exec.ordered.stream().map(o -> o.value).toList());

        // Now we can launch again (new future)
        CompletableFuture<TestOut> f2 = new CompletableFuture<>();
        Function<TestSeqEnv, CompletionStage<TestOut>> asyncFn2 = in -> f2;
        exec = new CapturingExecutor(1, direct, asyncFn2, failure, pm);
        assertEquals(AcceptResult.LAUNCHED, exec.execute(new TestSeqEnv("two")));
    }

    @Test
    void subtaskCap_M_blocks_when_no_permits_available() {
        CountingPermitManager pm = new CountingPermitManager(1);
        Executor direct = Runnable::run;

        CompletableFuture<TestOut> f = new CompletableFuture<>();
        Function<TestSeqEnv, CompletionStage<TestOut>> asyncFn = in -> f;

        FailureAdapter<TestSeqEnv, TestOut> failure = failureAdapterWithPrefix("err:");

        CapturingExecutor exec = new CapturingExecutor(4, direct, asyncFn, failure, pm);

        assertEquals(AcceptResult.LAUNCHED, exec.execute(new TestSeqEnv("a")));
        assertEquals(0, pm.availablePermits());

        // No M permits left → should reject immediately
        assertEquals(AcceptResult.REJECTED_FULL, exec.execute(new TestSeqEnv("b")));

        // Free M by completing first
        f.complete(new TestOut("ok:a"));
        exec.onWake();
        assertEquals(1, pm.availablePermits());
    }

    @Test
    void failurePath_uses_failureAdapter_and_orders_correctly() {
        CountingPermitManager pm = new CountingPermitManager(10);
        Executor direct = Runnable::run;

        CompletableFuture<TestOut> f0 = new CompletableFuture<>();
        CompletableFuture<TestOut> f1 = new CompletableFuture<>();

        AtomicInteger idx = new AtomicInteger(0);
        Function<TestSeqEnv, CompletionStage<TestOut>> asyncFn = in -> (idx.getAndIncrement() == 0 ? f0 : f1);

        FailureAdapter<TestSeqEnv, TestOut> failure = failureAdapterWithPrefix("fail:");

        CapturingExecutor exec = new CapturingExecutor(4, direct, asyncFn, failure, pm);

        // Launch two; complete second successfully, first exceptionally
        assertEquals(AcceptResult.LAUNCHED, exec.execute(new TestSeqEnv("x")));
        assertEquals(AcceptResult.LAUNCHED, exec.execute(new TestSeqEnv("y")));

        f1.complete(new TestOut("ok:y"));             // out-of-order success
        exec.onWake();                                 // should not drain yet
        f0.completeExceptionally(new RuntimeException("boom"));
        exec.onWake();                                 // now drains: fail:x, ok:y

        assertEquals(List.of("fail:x", "ok:y"),
                exec.ordered.stream().map(o -> o.value).toList());

        // Permits paired
        assertEquals(pm.acquired.get(), pm.released.get());
        assertEquals(pm.maxPermits(), pm.availablePermits());
    }

    @Test
    void needsWake_true_when_inFlight_or_inbox_nonEmpty() {
        CountingPermitManager pm = new CountingPermitManager(10);
        Executor direct = Runnable::run;

        CompletableFuture<TestOut> f = new CompletableFuture<>();
        Function<TestSeqEnv, CompletionStage<TestOut>> asyncFn = in -> f;
        FailureAdapter<TestSeqEnv, TestOut> failure = failureAdapterWithPrefix("fail:");

        CapturingExecutor exec = new CapturingExecutor(4, direct, asyncFn, failure, pm);

        assertFalse(exec.needsWake());
        assertEquals(AcceptResult.LAUNCHED, exec.execute(new TestSeqEnv("a")));
        assertTrue(exec.needsWake()); // inFlight>0

        f.complete(new TestOut("ok:a"));
        // Inbox now non-empty (direct executor enqueued), still needs wake until we drain
        assertTrue(exec.needsWake());
        exec.onWake();
        assertFalse(exec.needsWake());
    }

    @Test
    void sequenceIsPropagatedToOut() {
        CountingPermitManager pm = new CountingPermitManager(10);
        Executor direct = Runnable::run;

        Function<TestSeqEnv, CompletionStage<TestOut>> asyncFn =
                in -> CompletableFuture.completedFuture(new TestOut("v:" + in.value));
        FailureAdapter<TestSeqEnv, TestOut> failure = failureAdapterWithPrefix("err:");

        CapturingExecutor exec = new CapturingExecutor(4, direct, asyncFn, failure, pm);

        TestSeqEnv a = new TestSeqEnv("a");
        TestSeqEnv b = new TestSeqEnv("b");
        assertEquals(AcceptResult.LAUNCHED, exec.execute(a));
        assertEquals(AcceptResult.LAUNCHED, exec.execute(b));

        exec.onWake();

        // Ensure strict ordering AND that Out.seq == assigned input seq
        assertEquals(2, exec.ordered.size());
        assertEquals("v:a", exec.ordered.get(0).value);
        assertEquals(SeqTC.IN_SEQ.get(a), SeqTC.OUT_SEQ.get(exec.ordered.get(0)));
        assertEquals("v:b", exec.ordered.get(1).value);
        assertEquals(SeqTC.IN_SEQ.get(b), SeqTC.OUT_SEQ.get(exec.ordered.get(1)));
    }

    @Test
    void handleOrderedThrows_capacityIsStillReleased_andDrainContinues() {
        CountingPermitManager pm = new CountingPermitManager(10);
        Executor direct = Runnable::run;

        CompletableFuture<TestOut> f0 = new CompletableFuture<>();
        CompletableFuture<TestOut> f1 = new CompletableFuture<>();

        AtomicInteger idx = new AtomicInteger(0);
        Function<TestSeqEnv, CompletionStage<TestOut>> asyncFn = in -> (idx.getAndIncrement() == 0 ? f0 : f1);
        FailureAdapter<TestSeqEnv, TestOut> failure = failureAdapterWithPrefix("err:");

        // Override handleOrdered to throw when encountering "crash"
        CapturingExecutor exec = new CapturingExecutor(4, direct, asyncFn, failure, pm) {
            @Override protected void handleOrdered(TestOut out) {
                if ("ok:crash".equals(out.value)) throw new RuntimeException("boom");
                super.handleOrdered(out);
            }
        };

        assertEquals(AcceptResult.LAUNCHED, exec.execute(new TestSeqEnv("crash"))); // seq 0
        assertEquals(AcceptResult.LAUNCHED, exec.execute(new TestSeqEnv("next")));  // seq 1

        // Complete second first (out-of-order)
        f1.complete(new TestOut("ok:next"));
        exec.onWake();
        assertTrue(exec.ordered.isEmpty()); // waiting for 0

        // Now complete first; handleOrdered will throw for it, but capacity must still be released,
        // and seq 1 must drain afterwards.
        f0.complete(new TestOut("ok:crash"));
        int acquiredBefore = pm.acquired.get();

        exec.onWake();

        // "next" should still drain and be recorded, despite exception on first
        assertEquals(List.of("ok:next"), exec.ordered.stream().map(o -> o.value).toList());

        // permits must be balanced after both items processed
        assertEquals(pm.acquired.get(), pm.released.get());
        assertEquals(pm.maxPermits(), pm.availablePermits());
        assertEquals(acquiredBefore, pm.acquired.get()); // no new acquires during onWake
    }

    @Test
    void kEqualsThree_ringRoundsUpAndOrderingStillCorrect() {
        // K=3 (ring rounds to 4). Launch 3, complete out of order.
        CountingPermitManager pm = new CountingPermitManager(10);
        Executor direct = Runnable::run;

        CompletableFuture<TestOut> f0 = new CompletableFuture<>();
        CompletableFuture<TestOut> f1 = new CompletableFuture<>();
        CompletableFuture<TestOut> f2 = new CompletableFuture<>();

        AtomicInteger idx = new AtomicInteger(0);
        Function<TestSeqEnv, CompletionStage<TestOut>> asyncFn = in -> switch (idx.getAndIncrement()) {
            case 0 -> f0; case 1 -> f1; default -> f2;
        };
        FailureAdapter<TestSeqEnv, TestOut> failure = failureAdapterWithPrefix("err:");

        CapturingExecutor exec = new CapturingExecutor(3, direct, asyncFn, failure, pm);

        assertEquals(AcceptResult.LAUNCHED, exec.execute(new TestSeqEnv("a"))); // seq 0
        assertEquals(AcceptResult.LAUNCHED, exec.execute(new TestSeqEnv("b"))); // seq 1
        assertEquals(AcceptResult.LAUNCHED, exec.execute(new TestSeqEnv("c"))); // seq 2

        // Complete 2, then 0, then 1 → final drain must be a,b,c
        f2.complete(new TestOut("ok:c"));
        exec.onWake(); // nothing yet (waiting for 0)
        f0.complete(new TestOut("ok:a"));
        exec.onWake(); // drains a; still waiting for b
        f1.complete(new TestOut("ok:b"));
        exec.onWake(); // drains b, then c

        assertEquals(List.of("ok:a", "ok:b", "ok:c"),
                exec.ordered.stream().map(o -> o.value).toList());

        assertEquals(pm.acquired.get(), pm.released.get());
        assertEquals(pm.maxPermits(), pm.availablePermits());
    }

    @Test
    void mixedGating_kVsM_precedence() {
        // K=2, M=1 → second launch blocked by M (not K). After release, launch succeeds.
        CountingPermitManager pm = new CountingPermitManager(1);
        Executor direct = Runnable::run;

        CompletableFuture<TestOut> f0 = new CompletableFuture<>();
        Function<TestSeqEnv, CompletionStage<TestOut>> asyncFn = in -> f0;
        FailureAdapter<TestSeqEnv, TestOut> failure = failureAdapterWithPrefix("err:");

        CapturingExecutor exec = new CapturingExecutor(2, direct, asyncFn, failure, pm);

        assertEquals(AcceptResult.LAUNCHED, exec.execute(new TestSeqEnv("first")));  // takes M and counts toward K
        assertEquals(0, pm.availablePermits());
        assertEquals(AcceptResult.REJECTED_FULL, exec.execute(new TestSeqEnv("second"))); // blocked by M

        // Free M and ensure we can launch again
        f0.complete(new TestOut("ok:first"));
        exec.onWake();
        assertEquals(pm.maxPermits(), pm.availablePermits());

        // swap future & launch again
        CompletableFuture<TestOut> f1 = new CompletableFuture<>();
        CapturingExecutor exec2 = new CapturingExecutor(2, direct, in -> f1, failure, pm);
        assertEquals(AcceptResult.LAUNCHED, exec2.execute(new TestSeqEnv("second")));
    }

    @Test
    void executeAfterFinish_isRejected_andLateCompletionsIgnored() {
        CountingPermitManager pm = new CountingPermitManager(10);
        Executor direct = Runnable::run;

        CompletableFuture<TestOut> f = new CompletableFuture<>();
        Function<TestSeqEnv, CompletionStage<TestOut>> asyncFn = in -> f;
        FailureAdapter<TestSeqEnv, TestOut> failure = failureAdapterWithPrefix("err:");

        CapturingExecutor exec = new CapturingExecutor(4, direct, asyncFn, failure, pm);

        assertEquals(AcceptResult.LAUNCHED, exec.execute(new TestSeqEnv("a"))); // one in-flight
        exec.finish();

        // New executes must be rejected after finish
        assertEquals(AcceptResult.REJECTED_FULL, exec.execute(new TestSeqEnv("b")));

        // Complete the in-flight after finish; ring is finished → ignore completion
        f.complete(new TestOut("ok:a"));
        exec.onWake();
        assertTrue(exec.ordered.isEmpty(), "late completion should be ignored after finish");
    }
}
