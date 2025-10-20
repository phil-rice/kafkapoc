package com.hcltech.rmg.flinkadapters;


import com.hcltech.rmg.common.async.FailureAdapter;
import com.hcltech.rmg.flinkadapters.FlinkResultFutureTypeClass;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

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
    void completed_maps_to_single_result() {
        var fr = new FakeResultFuture<String>();
        var tc = new FlinkResultFutureTypeClass<String, String>(new StubFailures());

        tc.completed(fr, "OK");
        assertTrue(fr.done.get());
        assertEquals(List.of("OK"), fr.results);
        assertNull(fr.error);
    }

    @Test
    void timeout_maps_via_failure_adapter_by_default() {
        var fr = new FakeResultFuture<String>();
        var tc = new FlinkResultFutureTypeClass<String, String>(new StubFailures());

        tc.timedOut(fr, "X", 123L);
        assertTrue(fr.done.get());
        assertEquals(1, fr.results.size());
        assertEquals("TIMEOUT(X,123)", fr.results.get(0));
        assertNull(fr.error);
    }

    @Test
    void failure_maps_via_failure_adapter_by_default() {
        var fr = new FakeResultFuture<String>();
        var tc = new FlinkResultFutureTypeClass<String, String>(new StubFailures());

        tc.failed(fr, "Y", new IllegalStateException("boom"));
        assertTrue(fr.done.get());
        assertEquals(List.of("FAIL(Y,IllegalStateException)"), fr.results);
        assertNull(fr.error);
    }

    @Test
    void can_complete_exceptionally_on_timeout_when_configured() {
        var fr = new FakeResultFuture<String>();
        var tc = new FlinkResultFutureTypeClass<String, String>(new StubFailures(),
                false, /* failure */ true /* timeout */);

        tc.timedOut(fr, "Z", 999L);
        assertTrue(fr.done.get());
        assertTrue(fr.results.isEmpty());
        assertNotNull(fr.error);
        assertTrue(fr.error instanceof TimeoutException);
        assertTrue(fr.error.getMessage().contains("elapsedNanos=999"));
    }

    @Test
    void can_complete_exceptionally_on_failure_when_configured() {
        var fr = new FakeResultFuture<String>();
        var tc = new FlinkResultFutureTypeClass<String, String>(new StubFailures(),
                true /* failure */, false /* timeout */);

        var boom = new RuntimeException("oops");
        tc.failed(fr, "W", boom);
        assertTrue(fr.done.get());
        assertTrue(fr.results.isEmpty());
        assertSame(boom, fr.error);
    }

    @Test
    void nulls_are_rejected_and_adapter_must_not_return_null() {
        var tc = new FlinkResultFutureTypeClass<String, String>(new StubFailures());
        var fr = new FakeResultFuture<String>();

        assertThrows(NullPointerException.class, () -> tc.completed(null, "x"));
        assertThrows(NullPointerException.class, () -> tc.completed(fr, null));
        assertThrows(NullPointerException.class, () -> tc.failed(fr, "in", null));

        FailureAdapter<String, String> bad = new FailureAdapter<>() {
            @Override public String onFailure(String in, Throwable error) { return null; }
            @Override public String onTimeout(String in, long elapsedNanos) { return null; }
        };
        var tcBad = new FlinkResultFutureTypeClass<String, String>(bad);
        var fr1 = new FakeResultFuture<String>();
        assertThrows(NullPointerException.class, () -> tcBad.failed(fr1, "A", new RuntimeException()));
        var fr2 = new FakeResultFuture<String>();
        assertThrows(NullPointerException.class, () -> tcBad.timedOut(fr2, "B", 1L));
    }

    @Test
    void idempotency_complete_only_once() {
        var fr = new FakeResultFuture<String>();
        var tc = new FlinkResultFutureTypeClass<String, String>(new StubFailures());
        tc.completed(fr, "R1");
        assertThrows(IllegalStateException.class, () -> tc.completed(fr, "R2"));
        assertThrows(IllegalStateException.class, () -> tc.failed(fr, "in", new RuntimeException()));
        assertEquals(List.of("R1"), fr.results);
        assertNull(fr.error);
    }
}
