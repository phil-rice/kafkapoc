package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.common.async.*;
import com.hcltech.rmg.messages.*;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

public class DefaultPerKeyAsyncExecutorEnvelopeFailureTest {

    // ------- minimal builders -------
    private static EnvelopeHeader<Object> header(String domainId) {
        return new EnvelopeHeader<>(
                "domainType",
                "eventType",
                new RawMessage("raw", domainId, 0L, 0L, 0, 0L, "", "", ""),
                null,
                null
        );
    }

    private static Envelope<Object, String> ok(String domainId, String data) {
        return new ValueEnvelope<>(header(domainId), data, null, new ArrayList<>());
    }

    // ------- HasSeq for Envelope (reuse singleton) -------
    @SuppressWarnings("unchecked")
    private static final HasSeq<Envelope<Object, String>> SEQ =
            (HasSeq<Envelope<Object, String>>) (HasSeq<?>) HasSeqsForEnvelope.INSTANCE;

    // ------- simple PermitManager -------
    static final class CountingPermitManager implements PermitManager {
        private final Semaphore sem; private final int max;
        int acquired = 0, released = 0;
        CountingPermitManager(int max) { this.sem = new Semaphore(max); this.max = max; }
        @Override public boolean tryAcquire() { boolean ok = sem.tryAcquire(); if (ok) acquired++; return ok; }
        @Override public void release() { released++; sem.release(); }
        @Override public int availablePermits() { return sem.availablePermits(); }
        @Override public int maxPermits() { return max; }
    }

    // ------- capturing executor -------
    static final class CapturingExec
            extends DefaultPerKeyAsyncExecutor<Envelope<Object, String>, Envelope<Object, String>> {
        final List<Envelope<Object, String>> ordered = new ArrayList<>();
        CapturingExec(int K,
                      Executor exec,
                      Function<Envelope<Object, String>, CompletionStage<Envelope<Object, String>>> fn,
                      FailureAdapter<Envelope<Object, String>, Envelope<Object, String>> failure,
                      PermitManager pm) {
            super(K, exec, fn, failure, pm, SEQ, SEQ);
        }
        @Override protected void handleOrdered(Envelope<Object, String> out) { ordered.add(out); }
    }

    @Test
    void failure_then_success_outOfOrder_is_mapped_and_emitted_in_seq_order() {
        CountingPermitManager pm = new CountingPermitManager(10);
        Executor direct = Runnable::run;

        // futures controlled by test
        CompletableFuture<Envelope<Object, String>> f0 = new CompletableFuture<>();
        CompletableFuture<Envelope<Object, String>> f1 = new CompletableFuture<>();

        // async function: first call -> f0, second call -> f1
        final int[] idx = {0};
        Function<Envelope<Object, String>, CompletionStage<Envelope<Object, String>>> asyncFn =
                in -> (idx[0]++ == 0 ? f0 : f1);

        // Use the real adapter (fresh VE, preserves header/data, stageName="async")
        FailureAdapter<Envelope<Object, String>, Envelope<Object, String>> failure =
                EnvelopeFailureAdapter.defaultAdapter("async");

        CapturingExec exec = new CapturingExec(4, direct, asyncFn, failure, pm);

        // Launch two inputs for SAME key "K"
        Envelope<Object, String> in0 = ok("K", "x");      // seq0
        Envelope<Object, String> in1 = ok("K", "y");      // seq1
        assertEquals(AcceptResult.LAUNCHED, exec.execute(in0));
        assertEquals(AcceptResult.LAUNCHED, exec.execute(in1));

        // Complete second (success) first: onWake shouldn't drain (blocked on seq0)
        f1.complete(ok("K", "ok:y"));
        exec.onWake();
        assertTrue(exec.ordered.isEmpty(), "still waiting for seq0");

        // Now fail first: onWake should drain failure(in0) then success(in1), in order
        f0.completeExceptionally(new RuntimeException("boom"));
        exec.onWake();

        assertEquals(2, exec.ordered.size(), "expected two outputs after onWake()");
        Envelope<Object, String> o0 = exec.ordered.get(0);
        Envelope<Object, String> o1 = exec.ordered.get(1);

        // First must be an ErrorEnvelope produced by EnvelopeFailureAdapter
        assertTrue(o0 instanceof ErrorEnvelope, "first must be failure for seq0");
        @SuppressWarnings("unchecked")
        var err0 = (ErrorEnvelope<Object, String>) o0;

        // stageName is the operator id we passed
        assertEquals("async", err0.stageName());

        // The adapter builds a FRESH ValueEnvelope, preserves header/data
        assertNotSame(in0.valueEnvelope(), err0.valueEnvelope(), "must build a fresh ValueEnvelope (no aliasing)");
        assertEquals(in0.valueEnvelope().header(), err0.valueEnvelope().header(), "header must be preserved");
        assertEquals("x", err0.valueEnvelope().data(), "data must be preserved on failure");

        // Error list is compact and includes class/message (not "FAIL:x" anymore)
        assertNotNull(err0.errors());
        assertFalse(err0.errors().isEmpty());
        assertTrue(err0.errors().get(0).contains("RuntimeException"), "first error entry should contain class");
        assertTrue(err0.errors().get(0).contains("boom"), "first error entry should contain message");

        // Second output is the success of in1
        assertTrue(o1 instanceof ValueEnvelope);
        assertEquals("ok:y", o1.valueEnvelope().data());

        // sequence increases per key
        assertTrue(SEQ.get(o0) < SEQ.get(o1), "sequence must increase per key");

        // permit accounting balanced
        assertEquals(pm.acquired, pm.released);
        assertEquals(pm.maxPermits(), pm.availablePermits());
    }
}
