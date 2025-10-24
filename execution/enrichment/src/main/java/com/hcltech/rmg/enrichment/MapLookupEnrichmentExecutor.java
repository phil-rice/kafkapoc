package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.config.enrich.MapLookupEnrichment;
import com.hcltech.rmg.execution.aspects.AspectExecutorAsync;
import com.hcltech.rmg.messages.MsgTypeClass;
import com.hcltech.rmg.messages.ValueEnvelope;
import com.hcltech.rmg.common.function.Callback;

import java.util.List;
import java.util.Objects;

/**
 * Synchronous (inline-callback) implementation of a map-lookup enrichment.
 * Builds a key from input field paths and looks up a new value; emits a CepEvent or null.
 */
public final class MapLookupEnrichmentExecutor<CepState, Msg>
        implements AspectExecutorAsync<MapLookupEnrichment, ValueEnvelope<CepState, Msg>, CepEvent> {

    private final MsgTypeClass<Msg, List<String>> msgTypeClass;

    public MapLookupEnrichmentExecutor(MsgTypeClass<Msg, List<String>> msgTypeClass) {
        this.msgTypeClass = Objects.requireNonNull(msgTypeClass, "msgTypeClass");
    }

    @Override
    public void call(String key,
                     MapLookupEnrichment enrichment,
                     ValueEnvelope<CepState, Msg> input,
                     Callback<? super CepEvent> cb) {
        try {
            StringBuilder builder = new StringBuilder();
            for (List<String> path : enrichment.inputs()) {
                Object value = msgTypeClass.getValueFromPath(input.data(), path);
                if (value == null) { cb.success(null); return; }
                if (builder.length() > 0) builder.append('.');
                builder.append(value);
            }

            Object newValue = enrichment.lookup().get(builder.toString());
            if (newValue == null) { cb.success(null); return; }

            CepEvent event = CepEvent.set(enrichment.output(), newValue);
            cb.success(event);
        } catch (Throwable t) {
            cb.failure(t);
        }
    }
}
