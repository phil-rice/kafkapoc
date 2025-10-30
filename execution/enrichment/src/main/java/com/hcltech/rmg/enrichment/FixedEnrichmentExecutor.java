package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.config.enrich.FixedEnrichment;
import com.hcltech.rmg.config.enrich.MapLookupEnrichment;
import com.hcltech.rmg.execution.aspects.AspectExecutor;
import com.hcltech.rmg.messages.MsgTypeClass;
import com.hcltech.rmg.messages.ValueEnvelope;

import java.util.List;

public class FixedEnrichmentExecutor<CepState, Msg> implements AspectExecutor<FixedEnrichment, ValueEnvelope<CepState, Msg>, CepEvent> {

    @Override
    public CepEvent execute(String key, FixedEnrichment enrichment, ValueEnvelope<CepState, Msg> input) {
        var newValue = enrichment.value();
        if (newValue == null) return null;
        var newEvent = CepEvent.set(enrichment.output(), newValue);
        return newEvent;
    }
}
