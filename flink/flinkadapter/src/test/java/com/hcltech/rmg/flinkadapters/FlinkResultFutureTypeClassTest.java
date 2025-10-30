package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.common.async.FailureAdapter;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;

final class FlinkResultFutureTypeClassTest {

    // Minimal fake ResultFuture to validate behavior/idempotency.
    static final class FakeResultFuture<T> implements ResultFuture<T> {
        final List<T> results = new ArrayList<>();
        Throwable error;
        final AtomicBoolean done = new AtomicBoolean(false);

        @Override
        public void complete(Collection<T> iterable) {
            if (!done.compareAndSet(false, true)) {
                throw new IllegalStateException("complete called more than once");
            }
            iterable.forEach(results::add);
        }

        @Override
        public void completeExceptionally(Throwable throwable) {
            if (!done.compareAndSet(false, true)) {
                throw new IllegalStateException("completeExceptionally called more than once");
            }
            this.error = throwable;
        }
    }

    static final class StubFailures implements FailureAdapter<String, String> {
        @Override public String onFailure(String in, Throwable error) { return "FAIL(" + in + "," + error.getClass().getSimpleName() + ")"; }
        @Override public String onTimeout(String in, long elapsedNanos) { return "TIMEOUT(" + in + "," + elapsedNanos + ")"; }
    }

    @Test
    void completed_maps_to_single_result_and_runs_hook() {
        var fr = new FakeResultFuture<String>();
        var tc = new FlinkResultFutureTypeClass<String, String>(new StubFailures());

        AtomicReference<String> hookIn = new AtomicReference<>();
        AtomicReference<String> hookOut = new AtomicReference<>();
        BiConsumer<String, String> onComplete = (in, out) -> { hookIn.set(in); hookOut.set(out); };

        tc.completed(fr, onComplete, "IN", "OK");

        assertTrue(fr.done.get());
        assertEquals(List.of("OK"), fr.results);
        assertNull(fr.error);

        assertEquals("IN", hookIn.get());
        assertEquals("OK", hookOut.get());
    }

    @Test
    void timeout_maps_via_failure_adapter_by_default_and_runs_hook() {
        var fr = new FakeResultFuture<String>();
        var tc = new FlinkResultFutureTypeClass<String, String>(new StubFailures());

        AtomicReference<String> hookIn = new AtomicReference<>();
        AtomicReference<String> hookOut = new AtomicReference<>();
        BiConsumer<String, String> onTimedOut = (in, out) -> { hookIn.set(in); hookOut.set(out); };

        tc.timedOut(fr, onTimedOut, "X", 123L);

        assertTrue(fr.done.get());
        assertEquals(1, fr.results.size());
        assertEquals("TIMEOUT(X,123)", fr.results.get(0));
        assertNull(fr.error);

        assertEquals("X", hookIn.get());
        assertEquals("TIMEOUT(X,123)", hookOut.get());
    }

    @Test
    void failure_maps_via_failure_adapter_by_default_and_runs_hook() {
        var fr = new FakeResultFuture<String>();
        var tc = new FlinkResultFutureTypeClass<String, String>(new StubFailures());

        AtomicReference<String> hookIn = new AtomicReference<>();
        AtomicReference<String> hookOut = new AtomicReference<>();
        BiConsumer<String, String> onFailed = (in, out) -> { hookIn.set(in); hookOut.set(out); };

        var boom = new IllegalStateException("boom");
        tc.failed(fr, onFailed, "Y", boom);

        assertTrue(fr.done.get());
        assertEquals(List.of("FAIL(Y,IllegalStateException)"), fr.results);
        assertNull(fr.error);

        assertEquals("Y", hookIn.get());
        assertEquals("FAIL(Y,IllegalStateException)", hookOut.get());
    }

    @Test
    void completes_exceptionally_on_timeout_when_adapter_returns_null() {
        var fr = new FakeResultFuture<String>();
        FailureAdapter<String, String> adapter = new FailureAdapter<>() {
            @Override public String onFailure(String in, Throwable error) { return "IGNORED"; }
            @Override public String onTimeout(String in, long elapsedNanos) { return null; }
        };
        var tc = new FlinkResultFutureTypeClass<String, String>(adapter);

        tc.timedOut(fr, null, "Z", 999L);

        assertTrue(fr.done.get());
        assertTrue(fr.results.isEmpty());
        assertNotNull(fr.error);
        assertTrue(fr.error instanceof TimeoutException);
        assertTrue(fr.error.getMessage().contains("999"));
        assertTrue(fr.error.getMessage().contains("ns")); // matches implementation message format
    }

    @Test
    void completes_exceptionally_on_failure_when_adapter_returns_null() {
        var fr = new FakeResultFuture<String>();
        FailureAdapter<String, String> adapter = new FailureAdapter<>() {
            @Override public String onFailure(String in, Throwable error) { return null; }
            @Override public String onTimeout(String in, long elapsedNanos) { return "IGNORED"; }
        };
        var tc = new FlinkResultFutureTypeClass<String, String>(adapter);

        var boom = new RuntimeException("oops");
        tc.failed(fr, null, "W", boom);

        assertTrue(fr.done.get());
        assertTrue(fr.results.isEmpty());
        assertSame(boom, fr.error);
    }

    @Test
    void nulls_are_rejected_and_adapter_nulls_lead_to_exceptional_completion() {
        var tc = new FlinkResultFutureTypeClass<String, String>(new StubFailures());
        var fr = new FakeResultFuture<String>();

        // Required null checks in implementation
        assertThrows(NullPointerException.class, () -> tc.completed(null, null, "in", "x"));
        assertThrows(NullPointerException.class, () -> tc.completed(fr, null, "in", null));
        assertThrows(NullPointerException.class, () -> tc.failed(fr, null, "in", null));

        // Adapter returning null -> exceptional completion (no NPE)
        FailureAdapter<String, String> bad = new FailureAdapter<>() {
            @Override public String onFailure(String in, Throwable error) { return null; }
            @Override public String onTimeout(String in, long elapsedNanos) { return null; }
        };
        var tcBad = new FlinkResultFutureTypeClass<String, String>(bad);

        var fr1 = new FakeResultFuture<String>();
        tcBad.failed(fr1, null, "A", new RuntimeException("boom"));
        assertTrue(fr1.done.get());
        assertTrue(fr1.results.isEmpty());
        assertNotNull(fr1.error);

        var fr2 = new FakeResultFuture<String>();
        tcBad.timedOut(fr2, null, "B", 1L);
        assertTrue(fr2.done.get());
        assertTrue(fr2.results.isEmpty());
        assertTrue(fr2.error instanceof TimeoutException);
    }

    @Test
    void idempotency_complete_only_once() {
        var fr = new FakeResultFuture<String>();
        var tc = new FlinkResultFutureTypeClass<String, String>(new StubFailures());

        tc.completed(fr, null, "IN", "R1");

        assertThrows(IllegalStateException.class, () -> tc.completed(fr, null, "IN2", "R2"));
        assertThrows(IllegalStateException.class, () -> tc.failed(fr, null, "IN3", new RuntimeException("oops")));

        assertEquals(List.of("R1"), fr.results);
        assertNull(fr.error);
    }

    @Test
    void constructor_rejects_null_failure_adapter() {
        assertThrows(NullPointerException.class, () -> new FlinkResultFutureTypeClass<String, String>(null));
    }
}
