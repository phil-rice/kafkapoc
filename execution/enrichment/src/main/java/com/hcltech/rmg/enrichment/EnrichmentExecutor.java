package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.common.function.Callback;
import com.hcltech.rmg.config.enrich.CsvEnrichment;
import com.hcltech.rmg.config.enrich.EnrichmentWithDependencies;
import com.hcltech.rmg.config.enrich.FixedEnrichment;
import com.hcltech.rmg.config.enrich.MapLookupEnrichment;
import com.hcltech.rmg.execution.aspects.AspectExecutorAsync;
import com.hcltech.rmg.execution.aspects.AspectExecutorRepository;
import com.hcltech.rmg.execution.aspects.AspectExecutorSync;
import com.hcltech.rmg.messages.MsgTypeClass;
import com.hcltech.rmg.messages.ValueEnvelope;

import java.util.List;

/**
 * Sync dispatcher for enrichment aspects.
 * Current enrichers are synchronous; we register them as sync and build a sync dispatcher.
 */
public final class EnrichmentExecutor<CepState, Msg>
        implements AspectExecutorAsync<EnrichmentWithDependencies, ValueEnvelope<CepState, Msg>, CepEvent> {

    private final AspectExecutorAsync<EnrichmentWithDependencies, ValueEnvelope<CepState, Msg>, CepEvent> dispatcher;

    private final AspectExecutorRepository<EnrichmentWithDependencies, ValueEnvelope<CepState, Msg>, CepEvent> repo =
            new AspectExecutorRepository<>();

    public EnrichmentExecutor(CepStateTypeClass<CepState> cepStateTypeClass,
                              MsgTypeClass<Msg, List<String>> msgTypeClass) {

        // Register current synchronous enrichers
        // (If any future enricher is async, registerAsync it and use the async dispatcher where needed.)
        repo.register(MapLookupEnrichment.class, new MapLookupEnrichmentExecutor<>(msgTypeClass));
        repo.register(FixedEnrichment.class, new FixedEnrichmentExecutor<>());
        repo.register(CsvEnrichment.class, new CsvEnrichmentExecutor<>(cepStateTypeClass, msgTypeClass));
        this.dispatcher = repo.build();
    }

    @Override
    public void call(String key, EnrichmentWithDependencies enrichmentWithDependencies, ValueEnvelope<CepState, Msg> input, Callback<? super CepEvent> cb) {
        dispatcher.call(key, enrichmentWithDependencies, input, cb);
    }
}
