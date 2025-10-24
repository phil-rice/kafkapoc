package com.hcltech.rmg.all_execution;

import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.common.function.Callback;
import com.hcltech.rmg.enrichment.IEnrichmentAspectExecutor;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class EnrichmentPipelineStepTest {

    // ---- small callback helper ----
    private static final class CapturingCb<T> implements Callback<T> {
        T value; Throwable error;
        @Override public void success(T v) { value = v; }
        @Override public void failure(Throwable t) { error = t; }
    }

    @SuppressWarnings("unchecked")
    private <ESC, CS, MSG, SCH, FRT, FFR, MP>
    AppContainer<ESC, CS, MSG, SCH, FRT, FFR, MP> containerWith(IEnrichmentAspectExecutor<CS, MSG> exec) {
        var container = mock(AppContainer.class);
        when(container.enrichmentExecutor()).thenReturn(exec);
        // schema check is not done in this step, so we don’t need nameToSchemaMap/rootConfig here
        return container;
    }

    @Test
    @DisplayName("Pass-through when envelope is not a ValueEnvelope")
    @SuppressWarnings("unchecked")
    void passthroughWhenNotValueEnvelope() {
        var exec = (IEnrichmentAspectExecutor<Object, Object>) mock(IEnrichmentAspectExecutor.class);
        var container = containerWith(exec);
        var step = new EnrichmentPipelineStep<>(container, "mod");

        Envelope<Object, Object> plain = mock(Envelope.class);
        var cb = new CapturingCb<Envelope<Object, Object>>();

        step.call(plain, cb);

        assertSame(plain, cb.value);
        assertNull(cb.error);
        verifyNoInteractions(exec);
    }

    @Test
    @DisplayName("Delegates to enrichment executor and propagates success")
    @SuppressWarnings("unchecked")
    void delegatesAndPropagatesSuccess() {
        var exec = (IEnrichmentAspectExecutor<Object, Object>) mock(IEnrichmentAspectExecutor.class);
        var container = containerWith(exec);
        var step = new EnrichmentPipelineStep<>(container, "mod");

        ValueEnvelope<Object, Object> veIn = mock(ValueEnvelope.class);
        ValueEnvelope<Object, Object> veOut = mock(ValueEnvelope.class);

        doAnswer(inv -> {
            // args: ValueEnvelope in, Callback cb
            var cb = (Callback<Envelope<Object, Object>>) inv.getArgument(1);
            cb.success(veOut);
            return null;
        }).when(exec).call(same(veIn), any());

        var topCb = new CapturingCb<Envelope<Object, Object>>();
        step.call((Envelope<Object, Object>) veIn, topCb);

        assertSame(veOut, topCb.value);
        assertNull(topCb.error);
        verify(exec, times(1)).call(same(veIn), any());
    }

    @Test
    @DisplayName("Delegates to enrichment executor and propagates failure")
    @SuppressWarnings("unchecked")
    void delegatesAndPropagatesFailure() {
        var exec = (IEnrichmentAspectExecutor<Object, Object>) mock(IEnrichmentAspectExecutor.class);
        var container = containerWith(exec);
        var step = new EnrichmentPipelineStep<>(container, "mod");

        ValueEnvelope<Object, Object> veIn = mock(ValueEnvelope.class);

        doAnswer(inv -> {
            var cb = (Callback<Envelope<Object, Object>>) inv.getArgument(1);
            cb.failure(new RuntimeException("boom"));
            return null;
        }).when(exec).call(same(veIn), any());

        var topCb = new CapturingCb<Envelope<Object, Object>>();
        step.call((Envelope<Object, Object>) veIn, topCb);

        assertNull(topCb.value);
        assertNotNull(topCb.error);
        assertTrue(topCb.error instanceof RuntimeException);
        assertTrue(topCb.error.getMessage().toLowerCase().contains("boom"));
        verify(exec, times(1)).call(same(veIn), any());
    }

    @Test
    @DisplayName("If executor throws instead of calling failure, step converts to failure")
    @SuppressWarnings("unchecked")
    void executorThrowsIsConvertedToFailure() {
        var exec = (IEnrichmentAspectExecutor<Object, Object>) mock(IEnrichmentAspectExecutor.class);
        var container = containerWith(exec);
        var step = new EnrichmentPipelineStep<>(container, "mod");

        ValueEnvelope<Object, Object> veIn = mock(ValueEnvelope.class);

        doThrow(new IllegalStateException("threw"))
                .when(exec).call(same(veIn), any());

        var topCb = new CapturingCb<Envelope<Object, Object>>();
        step.call((Envelope<Object, Object>) veIn, topCb);

        assertNull(topCb.value);
        assertNotNull(topCb.error);
        assertTrue(topCb.error instanceof IllegalStateException);
        assertTrue(topCb.error.getMessage().toLowerCase().contains("threw"));
        verify(exec, times(1)).call(same(veIn), any());
    }
}
