package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.common.async.FailureAdapter;
import com.hcltech.rmg.common.async.HasSeq;
import com.hcltech.rmg.messages.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

public class AsyncCepProcessFunctionValueEnvelopeIT {

    private KeyedOneInputStreamOperatorTestHarness<
            String,
            Envelope<Object, String>,
            Envelope<Object, String>
            > harness;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) harness.close();
    }

    // ---------- minimal envelope helpers ----------
    private static EnvelopeHeader<Object> header(String domainId) {
        return new EnvelopeHeader<>(
                "domainType",
                "eventType",
                new RawMessage("raw", domainId, 0L, 0L, 0, 0L, "", "", ""),
                null,
                null
        );
    }

    private static ValueEnvelope<Object, String> ve(String domainId, String data) {
        return new ValueEnvelope<>(header(domainId), data, null, new ArrayList<>());
    }

    private static Envelope<Object, String> ok(String domainId, String data) {
        return ve(domainId, data);
    }

    @SuppressWarnings("unchecked")
    private static final HasSeq<Envelope<Object, String>> SEQ =
            (HasSeq<Envelope<Object, String>>) (HasSeq<?>) HasSeqsForEnvelope.INSTANCE;

    private KeyedOneInputStreamOperatorTestHarness<String,
            Envelope<Object, String>,
            Envelope<Object, String>> mkHarness(
            AsyncCepProcessFunction<String, Envelope<Object, String>, Envelope<Object, String>> fn
    ) throws Exception {
        var op = new KeyedProcessOperator<>(fn);
        var h = new KeyedOneInputStreamOperatorTestHarness<>(
                op,
                Envelope::domainId,
                TypeInformation.of(String.class)
        );
        h.open();
        return h;
    }

    // ---------- robust drain helpers ----------
    private List<Envelope<Object, String>> drainAllRecordsOnce() {
        List<Envelope<Object, String>> outs = new ArrayList<>();
        Object o;
        while ((o = harness.getOutput().poll()) != null) {
            if (o instanceof StreamRecord<?> sr) {
                @SuppressWarnings("unchecked")
                Envelope<Object, String> v = (Envelope<Object, String>) sr.getValue();
                outs.add(v);
            }
        }
        return outs;
    }
    private List<Envelope<Object, String>> awaitRecords(int n) throws Exception {
        List<Envelope<Object, String>> out = new ArrayList<>(n);
        int ticks = 0;
        while (out.size() < n && ticks++ < 10) {
            harness.setProcessingTime(harness.getProcessingTime() + 1);
            out.addAll(drainAllRecordsOnce());
        }
        return out;
    }

    // ---------- ITs (success path) ----------

    @Test
    void outOfOrderCompletions_areDeliveredInOrder_andSeqPropagates() throws Exception {
        CompletableFuture<Envelope<Object, String>> f0 = new CompletableFuture<>();
        CompletableFuture<Envelope<Object, String>> f1 = new CompletableFuture<>();
        Deque<CompletableFuture<Envelope<Object, String>>> q = new ArrayDeque<>();
        q.add(f0); q.add(f1);

        Function<Envelope<Object, String>, CompletionStage<Envelope<Object, String>>> asyncFn =
                in -> Objects.requireNonNull(q.poll(), "no future");

        // use the real failure adapter (operator id arbitrary here)
        FailureAdapter<Envelope<Object, String>, Envelope<Object, String>> failure =
                EnvelopeFailureAdapter.defaultAdapter("it-operator");

        AsyncCepProcessFunction<String, Envelope<Object, String>, Envelope<Object, String>> fn =
                new AsyncCepProcessFunction<>(4, 8, Runnable::run, asyncFn, failure, SEQ, SEQ);
        harness = mkHarness(fn);

        harness.processElement(ve("A", "x"), 0);
        harness.processElement(ve("A", "y"), 1);

        // complete second first -> no emit yet
        f1.complete(ok("A", "ok:y"));
        assertTrue(awaitRecords(1).isEmpty());

        // complete first -> both drain in order
        f0.complete(ok("A", "ok:x"));
        List<Envelope<Object, String>> outs = awaitRecords(2);
        assertEquals(2, outs.size());
        assertEquals("ok:x", outs.get(0).valueEnvelope().data());
        assertEquals("ok:y", outs.get(1).valueEnvelope().data());
        assertTrue(SEQ.get(outs.get(0)) < SEQ.get(outs.get(1)));
    }

    @Test
    void twoKeys_interleave_but_eachKey_staysOrdered() throws Exception {
        // M=3 so A0, A1, B0 all launch
        CompletableFuture<Envelope<Object, String>> a0 = new CompletableFuture<>();
        CompletableFuture<Envelope<Object, String>> a1 = new CompletableFuture<>();
        CompletableFuture<Envelope<Object, String>> b0 = new CompletableFuture<>();
        Deque<CompletableFuture<Envelope<Object, String>>> qA = new ArrayDeque<>();
        Deque<CompletableFuture<Envelope<Object, String>>> qB = new ArrayDeque<>();
        qA.add(a0); qA.add(a1); qB.add(b0);

        Function<Envelope<Object, String>, CompletionStage<Envelope<Object, String>>> asyncFn =
                in -> in.domainId().equals("A") ? qA.removeFirst() : qB.removeFirst();

        // use the real failure adapter (operator id = "async")
        FailureAdapter<Envelope<Object, String>, Envelope<Object, String>> failure =
                EnvelopeFailureAdapter.defaultAdapter("async");

        harness = mkHarness(new AsyncCepProcessFunction<>(
                2, 3, Runnable::run, asyncFn, failure, SEQ, SEQ
        ));

        harness.processElement(ve("A", "a0"), 0);
        harness.processElement(ve("A", "a1"), 1);
        harness.processElement(ve("B", "b0"), 2);

        // B completes first
        b0.complete(ok("B", "B0"));
        var bOuts = awaitRecords(1);
        assertFalse(bOuts.isEmpty());
        assertEquals("B0", bOuts.get(0).valueEnvelope().data());

        // A completes out-of-order: still blocked on a0
        a1.complete(ok("A", "A1"));
        assertTrue(awaitRecords(1).isEmpty());

        a0.complete(ok("A", "A0"));
        var aOuts = awaitRecords(2);
        assertEquals(2, aOuts.size());
        assertEquals("A0", aOuts.get(0).valueEnvelope().data());
        assertEquals("A1", aOuts.get(1).valueEnvelope().data());
        assertTrue(SEQ.get(aOuts.get(0)) < SEQ.get(aOuts.get(1)));
    }
}
