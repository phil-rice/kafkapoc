package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.common.async.FailureAdapter;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

final class FlinkResultFutureAdapterTest {

    static final class FakeResultFuture<T> implements ResultFuture<T> {
        final List<T> results = new ArrayList<>();
        Throwable error;
        final AtomicBoolean done = new AtomicBoolean(false);


        @Override public void complete(Collection<T> iterable) {
            if (!done.compareAndSet(false, true)) throw new IllegalStateException("complete twice");
            iterable.forEach(results::add);
        }
        @Override public void completeExceptionally(Throwable t) {
            if (!done.compareAndSet(false, true)) throw new IllegalStateException("complete twice");
            error = t;
        }
    }

    static final class StubFailures implements FailureAdapter<String,String> {
        @Override public String onFailure(String in, Throwable e) { return "FAIL("+in+","+e.getClass().getSimpleName()+")"; }
        @Override public String onTimeout(String in, long ns) { return "TIMEOUT("+in+","+ns+")"; }
    }

    @Test void completed_emits_single_out() {
        var rf = new FakeResultFuture<String>();
        var tc = new FlinkResultFutureAdapter<String,String>(new StubFailures());
        tc.completed(rf, "OK");
        assertEquals(List.of("OK"), rf.results);
        assertNull(rf.error);
    }

    @Test void timeout_maps_by_default() {
        var rf = new FakeResultFuture<String>();
        var tc = new FlinkResultFutureAdapter<String,String>(new StubFailures());
        tc.timedOut(rf, "X", 123L);
        assertEquals(List.of("TIMEOUT(X,123)"), rf.results);
        assertNull(rf.error);
    }

    @Test void failure_maps_by_default() {
        var rf = new FakeResultFuture<String>();
        var tc = new FlinkResultFutureAdapter<String,String>(new StubFailures());
        tc.failed(rf, "Y", new IllegalStateException("boom"));
        assertEquals(List.of("FAIL(Y,IllegalStateException)"), rf.results);
    }

    @Test void can_complete_exceptionally_when_configured() {
        var rf1 = new FakeResultFuture<String>();
        var tc1 = new FlinkResultFutureAdapter<String,String>(new StubFailures(), true, false);
        var ex = new RuntimeException("oops");
        tc1.failed(rf1, "A", ex);
        assertTrue(rf1.results.isEmpty());
        assertSame(ex, rf1.error);

        var rf2 = new FakeResultFuture<String>();
        var tc2 = new FlinkResultFutureAdapter<String,String>(new StubFailures(), false, true);
        tc2.timedOut(rf2, "B", 9L);
        assertTrue(rf2.error instanceof TimeoutException);
    }

    @Test void nulls_rejected_and_adapter_must_not_return_null() {
        var rf = new FakeResultFuture<String>();
        var tc = new FlinkResultFutureAdapter<String,String>(new StubFailures());
        assertThrows(NullPointerException.class, () -> tc.completed(null, "x"));
        assertThrows(NullPointerException.class, () -> tc.completed(rf, null));
        assertThrows(NullPointerException.class, () -> {
            FailureAdapter<String,String> bad = new FailureAdapter<>() {
                @Override public String onFailure(String in, Throwable e) { return null; }
                @Override public String onTimeout(String in, long ns) { return null; }
            };
            new FlinkResultFutureAdapter<String,String>(bad).timedOut(new FakeResultFuture<>(), "Z", 1L);
        });
    }
}
