package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.config.enrich.FixedEnrichment;
import com.hcltech.rmg.execution.aspects.AspectExecutor;
import com.hcltech.rmg.messages.ValueEnvelope;

public class FixedEnrichmentExecutor<CepState, Msg> implements AspectExecutor<FixedEnrichment, ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> {
    @Override
    public ValueEnvelope<CepState, Msg> execute(String key, FixedEnrichment fixedEnrichment, ValueEnvelope<CepState, Msg> input) {
        return input;
    }
}
