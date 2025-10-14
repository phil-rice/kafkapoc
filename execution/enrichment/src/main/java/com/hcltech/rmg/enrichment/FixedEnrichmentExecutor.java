package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.config.enrich.FixedEnrichment;
import com.hcltech.rmg.execution.aspects.AspectExecutor;
import com.hcltech.rmg.messages.MsgTypeClass;
import com.hcltech.rmg.messages.ValueEnvelope;

import java.util.List;

public class FixedEnrichmentExecutor<CepState, Msg> implements AspectExecutor<FixedEnrichment, ValueEnvelope<CepState, Msg>, CepEvent> {

    private final MsgTypeClass<Msg, List<String>> msgTypeClass;

    public FixedEnrichmentExecutor(MsgTypeClass<Msg, List<String>> msgTypeClass) {
        this.msgTypeClass = msgTypeClass;
    }

    @Override
    public CepEvent execute(String key, FixedEnrichment fixedEnrichment, ValueEnvelope<CepState, Msg> input) {
        StringBuilder builder = new StringBuilder();
        for (List<String> path : fixedEnrichment.inputs()) {
            Object value = msgTypeClass.getValueFromPath(input.data(), path);
            if (value == null) return null;
            if (!builder.isEmpty()) builder.append('.');
            builder.append(value.toString());
        }
        var newValue = fixedEnrichment.lookup().get(builder.toString());
        if (newValue == null) return null;
        var newEvent = CepEvent.set(fixedEnrichment.output(), newValue);
        return newEvent;
    }
}
