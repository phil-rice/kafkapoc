package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.config.enrich.EnrichmentWithDependencies;
import com.hcltech.rmg.config.enrich.FixedEnrichment;
import com.hcltech.rmg.config.enrich.MapLookupEnrichment;
import com.hcltech.rmg.execution.aspects.AspectExecutorAsync;
import com.hcltech.rmg.execution.aspects.AspectExecutorRepository;
import com.hcltech.rmg.messages.MsgTypeClass;
import com.hcltech.rmg.messages.ValueEnvelope;
import com.hcltech.rmg.common.function.Callback;

import java.util.List;

/**
 * Async-shaped dispatcher for enrichment aspects. Current concrete enrichers are synchronous
 * and are registered via {@link AspectExecutorRepository#registerAsync}, so callbacks fire inline.
 * If/when an enricher becomes asynchronous, it can call its callback later.
 */
public final class EnrichmentExecutor<CepState, Msg>
        implements AspectExecutorAsync<EnrichmentWithDependencies, ValueEnvelope<CepState, Msg>, CepEvent> {

    private final AspectExecutorAsync<EnrichmentWithDependencies, ValueEnvelope<CepState, Msg>, CepEvent> dispatcher;

    public EnrichmentExecutor(MsgTypeClass<Msg, List<String>> msgTypeClass) {
        var repo = new AspectExecutorRepository<EnrichmentWithDependencies, ValueEnvelope<CepState, Msg>, CepEvent>();

        // Register current enrichers — both are synchronous but async-shaped.
        repo.registerAsync(MapLookupEnrichment.class, new MapLookupEnrichmentExecutor<>(msgTypeClass));
        repo.registerAsync(FixedEnrichment.class,      new FixedEnrichmentExecutor<>());

        this.dispatcher = repo.build();
    }

    @Override
    public void call(String key,
                     EnrichmentWithDependencies aspect,
                     ValueEnvelope<CepState, Msg> input,
                     Callback<? super CepEvent> cb) {
        // Delegates to the snapshot dispatcher (sync enrichers invoke cb inline)
        dispatcher.call(key, aspect, input, cb);
    }
}
