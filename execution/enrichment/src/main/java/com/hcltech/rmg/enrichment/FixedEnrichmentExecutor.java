package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.config.enrich.FixedEnrichment;
import com.hcltech.rmg.execution.aspects.AspectExecutorAsync;
import com.hcltech.rmg.messages.ValueEnvelope;
import com.hcltech.rmg.common.function.Callback;

/**
 * Synchronous (inline-callback) fixed-value enrichment.
 * Emits a CepEvent setting the configured output to the fixed value, or null if value is null.
 */
public final class FixedEnrichmentExecutor<CepState, Msg>
        implements AspectExecutorAsync<FixedEnrichment, ValueEnvelope<CepState, Msg>, CepEvent> {

    @Override
    public void call(String key,
                     FixedEnrichment enrichment,
                     ValueEnvelope<CepState, Msg> input,
                     Callback<? super CepEvent> cb) {
        try {
            Object newValue = enrichment.value();
            if (newValue == null) { cb.success(null); return; }
            CepEvent event = CepEvent.set(enrichment.output(), newValue);
            cb.success(event);
        } catch (Throwable t) {
            cb.failure(t);
        }
    }
}
