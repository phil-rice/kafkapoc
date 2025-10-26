package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.config.enrich.MapLookupEnrichment;
import com.hcltech.rmg.execution.aspects.AspectExecutorSync;
import com.hcltech.rmg.messages.MsgTypeClass;
import com.hcltech.rmg.messages.ValueEnvelope;

import java.util.List;

public class MapLookupEnrichmentExecutor<CepState, Msg> implements AspectExecutorSync<MapLookupEnrichment, ValueEnvelope<CepState, Msg>, CepEvent> {

    private final MsgTypeClass<Msg, List<String>> msgTypeClass;

    public MapLookupEnrichmentExecutor(MsgTypeClass<Msg, List<String>> msgTypeClass) {
        this.msgTypeClass = msgTypeClass;
    }

    @Override
    public CepEvent execute(String key, MapLookupEnrichment mapLookupEnrichment, ValueEnvelope<CepState, Msg> input) {
        StringBuilder builder = new StringBuilder();
        for (List<String> path : mapLookupEnrichment.inputs()) {
            Object value = msgTypeClass.getValueFromPath(input.data(), path);
            if (value == null) return null;
            if (!builder.isEmpty()) builder.append('.');
            builder.append(value.toString());
        }
        var newValue = mapLookupEnrichment.lookup().get(builder.toString());
        if (newValue == null) return null;
        var newEvent = CepEvent.set(mapLookupEnrichment.output(), newValue);
        return newEvent;
    }
}
