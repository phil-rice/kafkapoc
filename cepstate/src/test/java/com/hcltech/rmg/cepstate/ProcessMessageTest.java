// src/test/java/com/example/cepstate/ProcessMessageTest.java
package com.hcltech.rmg.cepstate;

import com.hcltech.rmg.cepstate.retry.IRetryBuckets;
import com.hcltech.rmg.cepstate.worklease.AcquireResult;
import com.hcltech.rmg.cepstate.worklease.FailResult;
import com.hcltech.rmg.cepstate.worklease.SuceedResult;
import com.hcltech.rmg.cepstate.worklease.WorkLease;
import com.hcltech.rmg.common.ITimeService;
import com.hcltech.rmg.optics.IOpticsEvent;
import org.apache.commons.jxpath.JXPathContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
final class ProcessMessageTest {

    @org.mockito.Mock
    WorkLease lease;

    @org.mockito.Mock
    CepState<JXPathContext, String> cepState;

    @org.mockito.Mock
    IRetryBuckets buckets;

    // simple fixed time service for tests
    private static final ITimeService time = () -> 1_000_000L;

    @Test
    void happyPath_appliesMutations_returnsSideEffects_and_succeeds() {
        // arrange
        when(lease.tryAcquire(eq("domain-1"), anyLong()))
                .thenReturn(new AcquireResult("test", "t1"));
        when(cepState.get(eq("domain-1"), eq("INIT")))
                .thenReturn(CompletableFuture.completedFuture("INIT"));
        // succeed returns no backlog hint
        when(lease.succeed(eq("domain-1"), eq("t1")))
                .thenReturn(new SuceedResult("lease.succeed.noBacklog", null));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<IOpticsEvent<JXPathContext>>> eventsCap = ArgumentCaptor.forClass(List.class);
        when(cepState.mutate(eq("domain-1"), eventsCap.capture()))
                .thenReturn(CompletableFuture.completedFuture(null));

        var ctx = new ProcessMessageContext<>(
                "test-topic",            // topic
                "INIT",                  // startState
                cepState,
                lease,
                buckets,                 // non-null to avoid NPEs
                time,
                (String m) -> "domain-1"
        );

        var events = List.<IOpticsEvent<JXPathContext>>of(dummyEvent(), dummyEvent());
        var sideFx = List.of("email", "metrics");
        ProcessMessage<JXPathContext, String, String, String> processor =
                (snap, msg) -> CompletableFuture.completedFuture(
                        new SideEffectsAndCepStateMutations<>(events, sideFx)
                );

        // act
        var out = ProcessMessage.processMessage(ctx, processor, "msg", 42L)
                .toCompletableFuture().join();

        // assert
        assertEquals(sideFx, out);
        verify(lease).succeed("domain-1", "t1");
        verify(lease, never()).fail(any(), any());
        verify(cepState).get("domain-1", "INIT");
        verify(cepState).mutate(eq("domain-1"), anyList());
        assertEquals(events, eventsCap.getValue());
        // no backlog → no bucket scheduling
        verifyNoInteractions(buckets);
    }

    @Test
    void queued_returnsNull_and_doesNothing() {
        when(lease.tryAcquire(eq("domain-2"), anyLong()))
                .thenReturn(new AcquireResult("Test")); // queued/terminal

        var ctx = new ProcessMessageContext<>(
                "test-topic",
                "INIT",
                cepState,
                lease,
                buckets,
                time,
                (String m) -> "domain-2"
        );

        var out = ProcessMessage.processMessage(
                ctx,
                (snap, msg) -> {
                    fail("processor must not run");
                    return CompletableFuture.completedFuture(null);
                },
                "msg", 7L
        ).toCompletableFuture().join();

        assertNull(out);
        verifyNoInteractions(cepState);
        verify(lease, never()).succeed(any(), any());
        verify(lease, never()).fail(any(), any());
        verifyNoInteractions(buckets);
    }

    @Test
    void process_syncThrow_marksFail_and_bubbles() {
        when(lease.tryAcquire(eq("d"), anyLong()))
                .thenReturn(new AcquireResult("test", "tok"));
        when(cepState.get(eq("d"), eq("S0")))
                .thenReturn(CompletableFuture.completedFuture("S0"));
        // fail result (willRetry=false; no next)
        when(lease.fail(eq("d"), eq("tok")))
                .thenReturn(new FailResult("lease.fail.retryScheduled", null, false, 1));

        var ctx = new ProcessMessageContext<>(
                "test-topic", "S0", cepState, lease, buckets, time, (String m) -> "d"
        );

        ProcessMessage<JXPathContext, String, String, String> processor = (snap, msg) -> {
            throw new IllegalStateException("boom");
        };

        var ex = assertThrows(RuntimeException.class,
                () -> ProcessMessage.processMessage(ctx, processor, "m", 1L).toCompletableFuture().join());

        assertTrue(ex.getCause() instanceof IllegalStateException);
        verify(lease).fail("d", "tok");
        verify(lease, never()).succeed(any(), any());
        verify(cepState, never()).mutate(any(), anyList());
        // willRetry=false → no bucket call
        verifyNoInteractions(buckets);
    }

    @Test
    void mutate_asyncFailure_marksFail_and_bubbles() {
        when(lease.tryAcquire(eq("d"), anyLong()))
                .thenReturn(new AcquireResult("test", "tok"));
        when(cepState.get(eq("d"), eq("S0")))
                .thenReturn(CompletableFuture.completedFuture("S0"));
        when(cepState.mutate(eq("d"), anyList()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalArgumentException("nope")));
        // failing here, choose willRetry=true to exercise bucket path
        when(lease.fail(eq("d"), eq("tok")))
                .thenReturn(new FailResult("lease.fail.retryScheduled", null, true, 2));

        var ctx = new ProcessMessageContext<>(
                "test-topic", "S0", cepState, lease, buckets, time, (String m) -> "d"
        );

        var events = List.<IOpticsEvent<JXPathContext>>of(dummyEvent());
        var sideFx = List.of("fx");
        ProcessMessage<JXPathContext, String, String, String> processor =
                (snap, msg) -> CompletableFuture.completedFuture(
                        new SideEffectsAndCepStateMutations<>(events, sideFx)
                );

        var ex = assertThrows(RuntimeException.class,
                () -> ProcessMessage.processMessage(ctx, processor, "m", 10L).toCompletableFuture().join());

        assertEquals("nope", ex.getCause().getMessage());
        verify(lease).fail("d", "tok");
        verify(lease, never()).succeed(any(), any());
        verify(cepState).mutate(eq("d"), anyList());
        // willRetry=true → bucket called with current offset
        verify(buckets).addToRetryBucket(eq("test-topic"), eq("d"), eq(10L), anyLong(), eq(2));
    }

    @Test
    void process_asyncFailure_marksFail_and_bubbles() {
        when(lease.tryAcquire(eq("d"), anyLong()))
                .thenReturn(new AcquireResult("test", "tok"));
        when(cepState.get(eq("d"), eq("S0")))
                .thenReturn(CompletableFuture.completedFuture("S0"));
        when(lease.fail(eq("d"), eq("tok")))
                .thenReturn(new FailResult("lease.fail.retryScheduled", null, true, 1));

        var ctx = new ProcessMessageContext<>(
                "test-topic", "S0", cepState, lease, buckets, time, (String m) -> "d"
        );

        ProcessMessage<JXPathContext, String, String, String> processor =
                (snap, msg) -> CompletableFuture.failedFuture(new RuntimeException("async-err"));

        var ex = assertThrows(RuntimeException.class,
                () -> ProcessMessage.processMessage(ctx, processor, "m", 3L).toCompletableFuture().join());

        assertEquals("async-err", ex.getCause().getMessage());
        verify(lease).fail("d", "tok");
        verify(lease, never()).succeed(any(), any());
        verify(cepState, never()).mutate(any(), anyList());
        verify(buckets).addToRetryBucket(eq("test-topic"), eq("d"), eq(3L), anyLong(), eq(1));
    }

    @Test
    void get_asyncFailure_marksFail_and_bubbles_before_process() {
        when(lease.tryAcquire(eq("d"), anyLong()))
                .thenReturn(new AcquireResult("test", "tok"));
        when(cepState.get(eq("d"), eq("S0")))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("get-fail")));
        when(lease.fail(eq("d"), eq("tok")))
                .thenReturn(new FailResult("lease.fail.retryScheduled", null, true, 1));

        var ctx = new ProcessMessageContext<>(
                "test-topic", "S0", cepState, lease, buckets, time, (String m) -> "d"
        );

        // processor must NOT run
        ProcessMessage<JXPathContext, String, String, String> processor = (snap, msg) -> {
            fail("process() must not be called when get() fails");
            return CompletableFuture.completedFuture(null);
        };

        var ex = assertThrows(RuntimeException.class,
                () -> ProcessMessage.processMessage(ctx, processor, "m", 9L).toCompletableFuture().join());

        assertEquals("get-fail", ex.getCause().getMessage());
        verify(lease).fail("d", "tok");
        verify(lease, never()).succeed(any(), any());
        verify(cepState, never()).mutate(any(), anyList());
        verify(buckets).addToRetryBucket(eq("test-topic"), eq("d"), eq(9L), anyLong(), eq(1));
    }

    @Test
    void empty_mutations_still_calls_mutate_and_succeeds() {
        when(lease.tryAcquire(eq("domain-3"), anyLong()))
                .thenReturn(new AcquireResult("test", "t3"));
        when(cepState.get(eq("domain-3"), eq("INIT")))
                .thenReturn(CompletableFuture.completedFuture("INIT"));
        when(lease.succeed(eq("domain-3"), eq("t3")))
                .thenReturn(new SuceedResult("lease.succeed.noBacklog", null));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<IOpticsEvent<JXPathContext>>> cap = ArgumentCaptor.forClass(List.class);
        when(cepState.mutate(eq("domain-3"), cap.capture()))
                .thenReturn(CompletableFuture.completedFuture(null));

        var ctx = new ProcessMessageContext<>(
                "test-topic", "INIT", cepState, lease, buckets, time, (String m) -> "domain-3"
        );

        ProcessMessage<JXPathContext, String, String, String> processor =
                (snap, msg) -> CompletableFuture.completedFuture(
                        new SideEffectsAndCepStateMutations<>(List.of(), List.of("sf"))
                );

        var out = ProcessMessage.processMessage(ctx, processor, "msg", 5L).toCompletableFuture().join();

        assertEquals(List.of("sf"), out);
        assertNotNull(cap.getValue(), "mutations list must not be null");
        assertTrue(cap.getValue().isEmpty(), "mutations list should be empty");
        verify(lease, times(1)).succeed("domain-3", "t3");
        verify(lease, never()).fail(any(), any());
        verifyNoInteractions(buckets);
    }

    // --- helpers ---
    private static IOpticsEvent<JXPathContext> dummyEvent() {
        // No behavior needed for these tests; mutate() is mocked.
        return state -> state; // identity
    }
}
