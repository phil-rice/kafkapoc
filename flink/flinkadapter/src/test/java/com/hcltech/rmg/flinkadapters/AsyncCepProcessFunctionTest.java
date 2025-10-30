package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.common.async.FailureAdapter;
import com.hcltech.rmg.common.async.HasSeq;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

// --- Simple env POJOs (no inheritance) ---
final class InEnv {
    private long seq;
    final String key;
    final String payload;
    InEnv(String key, String payload) { this.key = key; this.payload = payload; }
    long getSeq() { return seq; }
    void setSeq(long s) { this.seq = s; }
    @Override public String toString() { return "InEnv{k=" + key + ",p=" + payload + ",seq=" + seq + "}"; }
}

final class OutEnv {
    private long seq;
    final String key;
    final String value;
    OutEnv(String key, String value) { this.key = key; this.value = value; }
    String getKey() { return key; }
    String getValue() { return value; }
    long getSeq() { return seq; }
    void setSeq(long s) { this.seq = s; }
    @Override public String toString() { return "OutEnv{k=" + key + ",v=" + value + ",seq=" + seq + "}"; }
}

// --- HasSeq type-class instances for the POJOs ---
final class SeqTC {
    static final HasSeq<InEnv> IN_SEQ = new HasSeq<>() {
        @Override public long get(InEnv v) { return v.getSeq(); }
        @Override public InEnv set(InEnv v, long seq) { v.setSeq(seq); return v; }
    };
    static final HasSeq<OutEnv> OUT_SEQ = new HasSeq<>() {
        @Override public long get(OutEnv v) { return v.getSeq(); }
        @Override public OutEnv set(OutEnv v, long seq) { v.setSeq(seq); return v; }
    };
}

// --- Test wrapper that passes the type classes to the operator ---
class TestAsyncCepProcessFunction extends AsyncCepProcessFunction<String, InEnv, OutEnv> {
    TestAsyncCepProcessFunction(int K, int M, Executor exec,
                                Function<InEnv, CompletionStage<OutEnv>> asyncFn,
                                FailureAdapter<InEnv, OutEnv> failure) {
        super(K, M, exec, asyncFn, failure, SeqTC.IN_SEQ, SeqTC.OUT_SEQ);
    }
}

public class AsyncCepProcessFunctionTest {

    private KeyedOneInputStreamOperatorTestHarness<String, InEnv, OutEnv> harness;

    @AfterEach
    void tearDown() throws Exception { if (harness != null) harness.close(); }

    private KeyedOneInputStreamOperatorTestHarness<String, InEnv, OutEnv> mkHarness(
            AsyncCepProcessFunction<String, InEnv, OutEnv> fn) throws Exception {
        var op = new KeyedProcessOperator<>(fn);
        var h = new KeyedOneInputStreamOperatorTestHarness<>(
                op,
                (InEnv in) -> in.key,               // key selector
                TypeInformation.of(String.class)    // key type
        );
        h.open(); // calls open(OpenContext)
        return h;
    }

    // --------- small helpers to safely unwrap StreamRecord<OutEnv> from the output queue ---------

    @SuppressWarnings("unchecked")
    private StreamRecord<OutEnv> pollRecord() {
        Object o = harness.getOutput().poll();
        assertNotNull(o, "expected a StreamRecord, got null");
        assertTrue(o instanceof StreamRecord<?>, "expected StreamRecord, got " + (o == null ? "null" : o.getClass()));
        return (StreamRecord<OutEnv>) o;
    }

    private OutEnv pollOut() {
        return pollRecord().getValue();
    }

    // --------- FailureAdapter helpers (new interface) ---------

    /** Map failure/timeout to "prefix + payload". */
    private static FailureAdapter<InEnv, OutEnv> failureAdapterWithPrefix(String prefix) {
        return new FailureAdapter<>() {
            @Override public OutEnv onFailure(InEnv in, Throwable error) {
                return new OutEnv(in.key, prefix + in.payload);
            }
            @Override public OutEnv onTimeout(InEnv in, long elapsedNanos) {
                return new OutEnv(in.key, prefix + in.payload);
            }
        };
    }

    /** Map failure/timeout to a constant value (ignores payload). */
    private static FailureAdapter<InEnv, OutEnv> failureAdapterConst(String constant) {
        return new FailureAdapter<>() {
            @Override public OutEnv onFailure(InEnv in, Throwable error) {
                return new OutEnv(in.key, constant);
            }
            @Override public OutEnv onTimeout(InEnv in, long elapsedNanos) {
                return new OutEnv(in.key, constant);
            }
        };
    }

    // ---------------------------------- tests ----------------------------------

    @Test
    void outOfOrderCompletions_areDeliveredInOrder_andSeqPropagates() throws Exception {
        CompletableFuture<OutEnv> f0 = new CompletableFuture<>();
        CompletableFuture<OutEnv> f1 = new CompletableFuture<>();
        Deque<CompletableFuture<OutEnv>> queue = new ArrayDeque<>();
        queue.add(f0); queue.add(f1);

        AtomicInteger asyncCalls = new AtomicInteger(0);
        Function<InEnv, CompletionStage<OutEnv>> asyncFn = in -> {
            asyncCalls.incrementAndGet();
            return Objects.requireNonNull(queue.poll(), "no future");
        };
        FailureAdapter<InEnv, OutEnv> failure = failureAdapterWithPrefix("fail:");

        Executor direct = Runnable::run;
        var fn = new TestAsyncCepProcessFunction(4, 8, direct, asyncFn, failure);
        harness = mkHarness(fn);

        harness.processElement(new InEnv("A", "x"), 0);
        harness.processElement(new InEnv("A", "y"), 1);

        f1.complete(new OutEnv("A", "ok:y"));
        harness.setProcessingTime(harness.getProcessingTime() + 1);
        assertTrue(harness.getOutput().isEmpty(), "must not emit until seq0 ready");

        f0.complete(new OutEnv("A", "ok:x"));
        harness.setProcessingTime(harness.getProcessingTime() + 1);

        OutEnv o1 = pollOut();
        OutEnv o2 = pollOut();
        assertEquals("ok:x", o1.getValue());
        assertEquals("ok:y", o2.getValue());
        assertTrue(o1.getSeq() < o2.getSeq(), "sequence must be increasing per key");
        assertEquals(2, asyncCalls.get());
    }

    @Test
    void perKeyCapK_blocksLaunch_whenInFlightEqualsK() throws Exception {
        CompletableFuture<OutEnv> f = new CompletableFuture<>();
        AtomicInteger asyncCalls = new AtomicInteger(0);
        Function<InEnv, CompletionStage<OutEnv>> asyncFn = in -> { asyncCalls.incrementAndGet(); return f; };
        FailureAdapter<InEnv, OutEnv> failure = failureAdapterWithPrefix("fail:");

        var fn = new TestAsyncCepProcessFunction(1, 10, Runnable::run, asyncFn, failure);
        harness = mkHarness(fn);

        harness.processElement(new InEnv("K", "one"), 0);
        assertEquals(1, asyncCalls.get());

        harness.processElement(new InEnv("K", "two"), 1);
        assertEquals(1, asyncCalls.get(), "per-key K must gate launch");

        f.complete(new OutEnv("K", "ok:one"));
        harness.setProcessingTime(harness.getProcessingTime() + 1);
        OutEnv out = pollOut();
        assertEquals("ok:one", out.getValue());
    }

    @Test
    void subtaskCapM_blocksLaunch_whenNoPermitsAvailable() throws Exception {
        CompletableFuture<OutEnv> hold = new CompletableFuture<>();
        AtomicInteger asyncCalls = new AtomicInteger(0);
        Function<InEnv, CompletionStage<OutEnv>> asyncFn = in -> { asyncCalls.incrementAndGet(); return hold; };
        FailureAdapter<InEnv, OutEnv> failure = failureAdapterWithPrefix("fail:");

        var fn = new TestAsyncCepProcessFunction(2, 1, Runnable::run, asyncFn, failure);
        harness = mkHarness(fn);

        harness.processElement(new InEnv("A", "a1"), 0);
        assertEquals(1, asyncCalls.get());

        harness.processElement(new InEnv("B", "b1"), 1);
        assertEquals(1, asyncCalls.get(), "M should gate second launch");

        hold.complete(new OutEnv("A", "ok:a1"));
        harness.setProcessingTime(harness.getProcessingTime() + 1);
        OutEnv out = pollOut();
        assertEquals("ok:a1", out.getValue());
    }

    @Test
    void failurePath_usesFailureAdapter_andStillOrders() throws Exception {
        CompletableFuture<OutEnv> f0 = new CompletableFuture<>();
        CompletableFuture<OutEnv> f1 = new CompletableFuture<>();
        AtomicInteger idx = new AtomicInteger(0);

        Function<InEnv, CompletionStage<OutEnv>> asyncFn = in -> (idx.getAndIncrement() == 0 ? f0 : f1);
        FailureAdapter<InEnv, OutEnv> failure = failureAdapterWithPrefix("FAIL:");

        var fn = new TestAsyncCepProcessFunction(4, 8, Runnable::run, asyncFn, failure);
        harness = mkHarness(fn);

        harness.processElement(new InEnv("K", "x"), 0);
        harness.processElement(new InEnv("K", "y"), 1);

        f1.complete(new OutEnv("K", "ok:y"));
        harness.setProcessingTime(harness.getProcessingTime() + 1);
        assertTrue(harness.getOutput().isEmpty());

        f0.completeExceptionally(new RuntimeException("boom"));
        harness.setProcessingTime(harness.getProcessingTime() + 1);

        OutEnv o1 = pollOut();
        OutEnv o2 = pollOut();
        assertEquals("FAIL:x", o1.getValue());
        assertEquals("ok:y",   o2.getValue());
        assertTrue(o1.getSeq() < o2.getSeq());
    }

    @Test
    void twoKeys_interleave_but_eachKey_staysOrdered() throws Exception {
        // K=2 (per-key), M=3 (subtask) so all three launches can proceed
        CompletableFuture<OutEnv> a0 = new CompletableFuture<>();
        CompletableFuture<OutEnv> a1 = new CompletableFuture<>();
        CompletableFuture<OutEnv> b0 = new CompletableFuture<>();
        Deque<CompletableFuture<OutEnv>> qA = new ArrayDeque<>();
        Deque<CompletableFuture<OutEnv>> qB = new ArrayDeque<>();
        qA.add(a0); qA.add(a1); qB.add(b0);

        AtomicInteger calls = new AtomicInteger(0);
        Function<InEnv, CompletionStage<OutEnv>> asyncFn = in -> {
            calls.incrementAndGet();
            return (in.key.equals("A") ? qA : qB).removeFirst();
        };
        FailureAdapter<InEnv, OutEnv> failure = failureAdapterConst("FAIL");

        harness = mkHarness(new TestAsyncCepProcessFunction(2, 3, Runnable::run, asyncFn, failure));

        // Launch three in-flight ops: A0, A1, B0
        harness.processElement(new InEnv("A", "a0"), 0);
        harness.processElement(new InEnv("A", "a1"), 1);
        harness.processElement(new InEnv("B", "b0"), 2);
        assertEquals(3, calls.get(), "all three launches should be accepted with M=3");

        // Complete B first — should emit on the next tick without affecting A's ordering
        b0.complete(new OutEnv("B", "b0"));
        harness.setProcessingTime(harness.getProcessingTime() + 1);
        OutEnv bo = pollOut();
        assertEquals("B", bo.getKey());
        assertEquals("b0", bo.getValue());

        // Now finish A out of order; must emit a0 then a1
        a1.complete(new OutEnv("A", "a1"));
        harness.setProcessingTime(harness.getProcessingTime() + 1);
        assertTrue(harness.getOutput().isEmpty(), "A still blocked on a0");

        a0.complete(new OutEnv("A", "a0"));
        harness.setProcessingTime(harness.getProcessingTime() + 1);
        OutEnv ao0 = pollOut();
        OutEnv ao1 = pollOut();
        assertEquals("a0", ao0.getValue());
        assertEquals("a1", ao1.getValue());
        assertTrue(ao0.getSeq() < ao1.getSeq());
    }

    @Test
    void rejected_when_K_full_until_capacity_frees() throws Exception {
        CompletableFuture<OutEnv> hold = new CompletableFuture<>();
        AtomicInteger calls = new AtomicInteger();
        Function<InEnv, CompletionStage<OutEnv>> asyncFn = in -> { calls.incrementAndGet(); return hold; };
        FailureAdapter<InEnv, OutEnv> failure = failureAdapterConst("FAIL");

        harness = mkHarness(new TestAsyncCepProcessFunction(1, 10, Runnable::run, asyncFn, failure));

        harness.processElement(new InEnv("K", "x"), 0);
        harness.processElement(new InEnv("K", "y"), 1);
        harness.processElement(new InEnv("K", "z"), 2);

        assertEquals(1, calls.get(), "only first launch should succeed");
        assertTrue(harness.getOutput().isEmpty());

        hold.complete(new OutEnv("K", "x"));
        harness.setProcessingTime(harness.getProcessingTime() + 1);
        OutEnv o = pollOut();
        assertEquals("x", o.getValue());
    }

    @Test
    void alreadyCompletedFuture_emits_after_timer_tick() throws Exception {
        CompletableFuture<OutEnv> done = CompletableFuture.completedFuture(new OutEnv("A", "ok"));
        Function<InEnv, CompletionStage<OutEnv>> asyncFn = in -> done;
        FailureAdapter<InEnv, OutEnv> failure = failureAdapterConst("FAIL");

        harness = mkHarness(new TestAsyncCepProcessFunction(4, 8, Runnable::run, asyncFn, failure));
        harness.processElement(new InEnv("A", "x"), 0);
        assertTrue(harness.getOutput().isEmpty(), "no emit until timer fires");
        harness.setProcessingTime(harness.getProcessingTime() + 1);
        assertEquals("ok", pollOut().getValue());
    }

    static final class CountingPF extends TestAsyncCepProcessFunction {
        final AtomicInteger onTimerCalls = new AtomicInteger(0);
        CountingPF(int K, int M, Executor ex,
                   Function<InEnv, CompletionStage<OutEnv>> f,
                   FailureAdapter<InEnv, OutEnv> fail) { super(K, M, ex, f, fail); }
        @Override
        public void onTimer(
                long ts,
                KeyedProcessFunction<String, InEnv, OutEnv>.OnTimerContext ctx,
                Collector<OutEnv> out)  {
            onTimerCalls.incrementAndGet();
            super.onTimer(ts, ctx, out);
        }
    }

    @Test
    void only_one_timer_until_first_drain() throws Exception {
        CompletableFuture<OutEnv> hold = new CompletableFuture<>();
        Function<InEnv, CompletionStage<OutEnv>> asyncFn = in -> hold;
        FailureAdapter<InEnv, OutEnv> failure = failureAdapterConst("FAIL");

        CountingPF fn = new CountingPF(8, 8, Runnable::run, asyncFn, failure);
        harness = mkHarness(fn);

        harness.processElement(new InEnv("A", "x"), 0);
        harness.processElement(new InEnv("A", "y"), 1);
        harness.processElement(new InEnv("A", "z"), 2);

        harness.setProcessingTime(harness.getProcessingTime() + 1);
        assertEquals(1, fn.onTimerCalls.get(), "only one timer should have fired so far");
        assertTrue(harness.getOutput().isEmpty());

        hold.complete(new OutEnv("A", "x"));
        harness.setProcessingTime(harness.getProcessingTime() + 1);
        assertFalse(harness.getOutput().isEmpty());
    }

    @Test
    void syncException_in_asyncFn_routes_to_failureAdapter() throws Exception {
        Function<InEnv, CompletionStage<OutEnv>> asyncFn = in -> { throw new RuntimeException("boom"); };
        FailureAdapter<InEnv, OutEnv> failure = failureAdapterWithPrefix("FAIL:");

        harness = mkHarness(new TestAsyncCepProcessFunction(4, 8, Runnable::run, asyncFn, failure));
        harness.processElement(new InEnv("A", "x"), 0);
        harness.setProcessingTime(harness.getProcessingTime() + 1);
        assertEquals("FAIL:x", pollOut().getValue());
    }
}
