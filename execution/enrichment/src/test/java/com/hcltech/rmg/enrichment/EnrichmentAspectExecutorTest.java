package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.common.function.Callback;
import com.hcltech.rmg.config.enrich.EnrichmentWithDependencies;
import com.hcltech.rmg.execution.aspects.AspectExecutorAsync;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.*;

/**
 * Tests for the dynamic, parallel EnrichmentAspectExecutor:
 * - parallel within a generation
 * - deterministic fold order (by set iteration order)
 * - error aggregation
 * - skipping empty/no generations
 */
@SuppressWarnings({"unchecked", "rawtypes"})
class EnrichmentAspectExecutorTest {

    /** Minimal state type for generics. */
    static final class S {}

    /** Captures callbacks per enricher and lets tests complete them later. */
    static final class CapturingEvaluator
            implements AspectExecutorAsync<EnrichmentWithDependencies, ValueEnvelope<S, String>, CepEvent> {

        private final Map<EnrichmentWithDependencies, Callback<CepEvent>> callbacks = new HashMap<>();

        @Override
        public void call(String key,
                         EnrichmentWithDependencies component,
                         ValueEnvelope<S, String> in,
                         Callback<? super CepEvent> cb) {
            // Record the callback so tests can complete it later in any order
            callbacks.put(component, (Callback<CepEvent>) cb);
        }

        void completeSuccess(EnrichmentWithDependencies comp, CepEvent evt) {
            var c = callbacks.remove(comp);
            assertNotNull(c, "No callback registered for component");
            c.success(evt);
        }

        void completeFailure(EnrichmentWithDependencies comp, Throwable t) {
            var c = callbacks.remove(comp);
            assertNotNull(c, "No callback registered for component");
            c.failure(t);
        }
    }

    /** Simple capturing callback for the outer IEnrichmentAspectExecutor.call(...) */
    static final class RecordingCallback implements Callback<ValueEnvelope<S, String>> {
        final CountDownLatch done = new CountDownLatch(1);
        volatile ValueEnvelope<S, String> result;
        volatile Throwable error;

        @Override public void success(ValueEnvelope<S, String> value) { this.result = value; this.done.countDown(); }
        @Override public void failure(Throwable error)               { this.error  = error;  this.done.countDown(); }
    }

    private static <T> LinkedHashSet<T> linkedSetOf(T... items) {
        LinkedHashSet<T> s = new LinkedHashSet<>();
        Collections.addAll(s, items);
        return s;
    }

    /** Build an executor with a single generation (LinkedHashSet for stable order). */
    private static class Harness {
        final EnrichmentWithDependencies e1 = mock(EnrichmentWithDependencies.class, "e1");
        final EnrichmentWithDependencies e2 = mock(EnrichmentWithDependencies.class, "e2");
        final EnrichmentWithDependencies e3 = mock(EnrichmentWithDependencies.class, "e3");
        final CepStateTypeClass<S> cepType = mock(CepStateTypeClass.class);
        final CapturingEvaluator evaluator = new CapturingEvaluator();

        EnrichmentAspectExecutor<S, String> build(List<Set<EnrichmentWithDependencies>> gens) {
            return new EnrichmentAspectExecutor<>(cepType, gens, evaluator::call);
        }
    }

    @Test
    void parallel_success_in_generation_folds_in_deterministic_order_even_if_callbacks_arrive_out_of_order() throws Exception {
        // Arrange: one generation with two enrichers in a specific (stable) order
        var h = new Harness();
        var gen = linkedSetOf(h.e1, h.e2);
        var exec = h.build(List.of(gen));

        // Prepare a chain of envelopes to observe fold order:
        // acc0 -(evt from e1)-> acc1 -(evt from e2)-> acc2
        ValueEnvelope<S, String> acc0 = mock(ValueEnvelope.class, "acc0");
        ValueEnvelope<S, String> acc1 = mock(ValueEnvelope.class, "acc1");
        ValueEnvelope<S, String> acc2 = mock(ValueEnvelope.class, "acc2");
        CepEvent evt1 = mock(CepEvent.class, "evt1");
        CepEvent evt2 = mock(CepEvent.class, "evt2");

        when(acc0.withNewCepEvent(any(), same(evt1))).thenReturn(acc1);
        when(acc1.withNewCepEvent(any(), same(evt2))).thenReturn(acc2);

        var outer = new RecordingCallback();

        // Act: kick off the executor; it will register two callbacks and return immediately
        exec.call(acc0, outer);

        // Now complete callbacks OUT OF ORDER: second first, then first.
        h.evaluator.completeSuccess(h.e2, evt2);
        // At this point, fold should not yet have happened (waiting for e1)
        assertEquals(1, outer.done.getCount(), "Outer callback should not fire before all inner callbacks complete");

        h.evaluator.completeSuccess(h.e1, evt1);

        // Assert: outer.success invoked once with the folded envelope acc2
        assertTrue(outer.done.await(500, java.util.concurrent.TimeUnit.MILLISECONDS), "Outer callback not invoked");
        assertNull(outer.error);
        assertSame(acc2, outer.result, "Folding must respect generation order (e1 then e2)");
    }

    @Test
    void aggregates_failures_and_fails_generation_once() throws Exception {
        var h = new Harness();
        var gen = linkedSetOf(h.e1, h.e2);
        var exec = h.build(List.of(gen));

        ValueEnvelope<S, String> acc0 = mock(ValueEnvelope.class);
        var outer = new RecordingCallback();

        exec.call(acc0, outer);

        // Fail both enrichers in any order
        RuntimeException e1 = new RuntimeException("boom1");
        RuntimeException e2 = new RuntimeException("boom2");
        h.evaluator.completeFailure(h.e2, e2);
        h.evaluator.completeFailure(h.e1, e1);

        assertTrue(outer.done.await(500, java.util.concurrent.TimeUnit.MILLISECONDS), "Outer callback not invoked");
        assertNotNull(outer.error);
        assertTrue(outer.error instanceof EnrichmentAspectExecutor.EnrichmentBatchException);
        var agg = (EnrichmentAspectExecutor.EnrichmentBatchException) outer.error;
        assertEquals(0, agg.getMessage().contains("generation 0") ? 0 : -1, "Should mention generation 0 in message");
        assertEquals(2, agg.failures().size(), "Both failures should be present");
    }

    @Test
    void no_generations_passes_through_input() throws Exception {
        var h = new Harness();
        var exec = h.build(List.of()); // no generations

        ValueEnvelope<S, String> acc0 = mock(ValueEnvelope.class);
        var outer = new RecordingCallback();

        exec.call(acc0, outer);

        assertTrue(outer.done.await(100, TimeUnit.MILLISECONDS), "Outer callback not invoked");
        assertNull(outer.error);
        assertSame(acc0, outer.result, "Input should pass through unchanged when no generations");
    }

    @Test
    void skips_empty_generation_then_processes_next_generation() throws Exception {
        var h = new Harness();
        var empty = Collections.<EnrichmentWithDependencies>emptySet();
        var gen2  = linkedSetOf(h.e1, h.e2); // stable order e1 -> e2
        var exec  = h.build(List.of(empty, gen2));

        // Prepare envelope chain for deterministic fold: acc0 -(e1)-> acc1 -(e2)-> acc2
        ValueEnvelope<S, String> acc0 = mock(ValueEnvelope.class, "acc0");
        ValueEnvelope<S, String> acc1 = mock(ValueEnvelope.class, "acc1");
        ValueEnvelope<S, String> acc2 = mock(ValueEnvelope.class, "acc2");
        CepEvent evt1 = mock(CepEvent.class, "evt1");
        CepEvent evt2 = mock(CepEvent.class, "evt2");

        when(acc0.withNewCepEvent(any(), same(evt1))).thenReturn(acc1);
        when(acc1.withNewCepEvent(any(), same(evt2))).thenReturn(acc2);

        var outer = new RecordingCallback();

        // Act: start; empty gen should be skipped transparently; callbacks registered for gen2
        exec.call(acc0, outer);

        // Complete in reverse order to ensure deterministic fold by set iteration
        h.evaluator.completeSuccess(h.e2, evt2);
        assertEquals(1, outer.done.getCount(), "Should not complete until all gen2 items done");
        h.evaluator.completeSuccess(h.e1, evt1);

        // Assert final folded envelope is acc2
        assertTrue(outer.done.await(500, java.util.concurrent.TimeUnit.MILLISECONDS), "Outer callback not invoked");
        assertNull(outer.error);
        assertSame(acc2, outer.result);
    }
}
