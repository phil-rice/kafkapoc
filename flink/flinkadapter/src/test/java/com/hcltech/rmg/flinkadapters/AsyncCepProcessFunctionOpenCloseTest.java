package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.common.async.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the leak-safe open()/close() semantics:
 * - owned thread pool is created and properly shut down
 * - external executors are not shut down
 * - per-key executors are finished/cleared by safeCleanup()/close()
 */
public class AsyncCepProcessFunctionOpenCloseTest {

    // Simple In/Out + HasSeq typeclasses for testing
    static final class In {
        long seq;
        final String key;
        final String v;

        In(String k, String v) { this.key = k; this.v = v; }
        long getSeq() { return seq; }
        void setSeq(long s) { seq = s; }
    }

    static final class Out {
        long seq;
        final String key;
        final String v;

        Out(String k, String v) { this.key = k; this.v = v; }
        long getSeq() { return seq; }
        void setSeq(long s) { seq = s; }
    }

    static final HasSeq<In> IN_SEQ = new HasSeq<>() {
        public long get(In x) { return x.getSeq(); }
        public In set(In x, long s) { x.setSeq(s); return x; }
    };
    static final HasSeq<Out> OUT_SEQ = new HasSeq<>() {
        public long get(Out x) { return x.getSeq(); }
        public Out set(Out x, long s) { x.setSeq(s); return x; }
    };

    private KeyedOneInputStreamOperatorTestHarness<String, In, Out> harness;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) harness.close();
    }

    private KeyedOneInputStreamOperatorTestHarness<String, In, Out> mkHarness(
            AsyncCepProcessFunction<String, In, Out> fn) throws Exception {
        var op = new KeyedProcessOperator<>(fn);
        var h = new KeyedOneInputStreamOperatorTestHarness<>(
                op,
                (In in) -> in.key,
                TypeInformation.of(String.class)
        );
        h.open();
        return h;
    }

    @Test
    void ownedPool_created_on_open_and_shutdown_on_close() throws Exception {
        Function<In, CompletionStage<Out>> asyncFn =
                in -> CompletableFuture.completedFuture(new Out(in.key, "ok:" + in.v));
        // real adapter for POJOs
        FailureAdapter<In, Out> failure =
                PojoFailureAdapter.same(in -> new Out(in.key, "fail:" + in.v));

        AsyncCepProcessFunction<String, In, Out> fn = new AsyncCepProcessFunction<>(
                8, 64, null, asyncFn, failure, IN_SEQ, OUT_SEQ
        );
        harness = mkHarness(fn);

        ThreadPoolExecutor owned = fn.getOwnedPoolForTest();
        assertNotNull(owned, "ownedPool must be created");
        assertFalse(owned.isShutdown(), "ownedPool must be running");

        // do minimal work to create per-key state
        harness.processElement(new In("A", "x"), 0);
        harness.setProcessingTime(harness.getProcessingTime() + 1);
        Object o = harness.getOutput().poll();
        if (o instanceof StreamRecord<?> sr) {
            // drain
        }

        // close harness → operator.close() should shutdown owned pool & clear state
        harness.close();
        owned = fn.getOwnedPoolForTest();
        assertNull(owned, "ownedPool must be nulled after close");

        Map<String, ?> ctx = fn.getCtxByKeyForTest();
        assertNull(ctx, "ctxByKey must be nulled after close");
    }

    @Test
    void safeCleanup_finishes_executors_and_shuts_pool() throws Exception {
        Function<In, CompletionStage<Out>> asyncFn = in -> new CompletableFuture<>(); // hold forever
        FailureAdapter<In, Out> failure =
                PojoFailureAdapter.same(in -> new Out(in.key, "fail:" + in.v));

        AsyncCepProcessFunction<String, In, Out> fn = new AsyncCepProcessFunction<>(
                2, 8, null, asyncFn, failure, IN_SEQ, OUT_SEQ
        );
        harness = mkHarness(fn);

        harness.processElement(new In("K", "one"), 0);

        assertNotNull(fn.getOwnedPoolForTest());
        assertNotNull(fn.getCtxByKeyForTest());
        assertFalse(fn.getCtxByKeyForTest().isEmpty(), "should have a KeyCtx");

        fn.safeCleanup();

        assertNull(fn.getOwnedPoolForTest(), "ownedPool must be nulled after safeCleanup");
        assertNull(fn.getCtxByKeyForTest(), "ctxByKey must be nulled after safeCleanup");

        // It should be legal to open() again via harness (same operator instance)
        harness.close();
        harness = mkHarness(fn);
        assertNotNull(fn.getOwnedPoolForTest(), "ownedPool must be rebuilt on re-open");
        assertNotNull(fn.getCtxByKeyForTest(), "ctxByKey must be rebuilt on re-open");
    }

    static final class NotShutDownExec extends AbstractExecutorService {
        volatile boolean shutdownCalled = false;
        @Override public void shutdown() { shutdownCalled = true; }
        @Override public java.util.List<Runnable> shutdownNow() { shutdownCalled = true; return java.util.List.of(); }
        @Override public boolean isShutdown() { return shutdownCalled; }
        @Override public boolean isTerminated() { return shutdownCalled; }
        @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return true; }
        @Override public void execute(Runnable command) { command.run(); }
    }

    @Test
    void externalExecutor_is_NOT_shutdown_on_close() throws Exception {
        NotShutDownExec external = new NotShutDownExec();

        Function<In, CompletionStage<Out>> asyncFn =
                in -> CompletableFuture.completedFuture(new Out(in.key, "ok:" + in.v));
        FailureAdapter<In, Out> failure =
                PojoFailureAdapter.same(in -> new Out(in.key, "fail:" + in.v));

        AsyncCepProcessFunction<String, In, Out> fn = new AsyncCepProcessFunction<>(
                8, 64, external, asyncFn, failure, IN_SEQ, OUT_SEQ
        );
        harness = mkHarness(fn);

        harness.processElement(new In("X", "y"), 0);
        harness.setProcessingTime(harness.getProcessingTime() + 1);
        harness.close();

        assertFalse(external.shutdownCalled, "external executor must NOT be shutdown by operator.close()");
        assertNull(fn.getOwnedPoolForTest(), "no ownedPool must exist when using external executor");
    }
}
