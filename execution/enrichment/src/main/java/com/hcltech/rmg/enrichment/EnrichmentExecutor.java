package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.common.apiclient.ApiClient;
import com.hcltech.rmg.config.enrich.*;
import com.hcltech.rmg.execution.aspects.AspectExecutor;
import com.hcltech.rmg.execution.aspects.AspectExecutorRepository;
import com.hcltech.rmg.messages.MsgTypeClass;
import com.hcltech.rmg.messages.ValueEnvelope;

import java.util.List;

public class EnrichmentExecutor<CepState, Msg> implements AspectExecutor<EnrichmentWithDependencies, ValueEnvelope<CepState, Msg>, CepEvent> {
    private final AspectExecutor<EnrichmentWithDependencies, ValueEnvelope<CepState, Msg>, CepEvent> executor;
    AspectExecutorRepository<EnrichmentWithDependencies, ValueEnvelope<CepState, Msg>, CepEvent> aspectRepository = new AspectExecutorRepository<>();


    public EnrichmentExecutor(ApiClient<String, CepState> apiClient, CepStateTypeClass<CepState> cepStateTypeClass, MsgTypeClass<Msg, List<String>> msgTypeClass) {
        aspectRepository.register(MapLookupEnrichment.class, new MapLookupEnrichmentExecutor<CepState, Msg>(msgTypeClass));
        aspectRepository.register(FixedEnrichment.class, new FixedEnrichmentExecutor<>());
        aspectRepository.register(CsvEnrichment.class, new CsvEnrichmentExecutor<>(cepStateTypeClass, msgTypeClass));
        aspectRepository.register(CsvFromAzureEnrichment.class, new CsvFromAzureEnrichmentExecutor<>(cepStateTypeClass, msgTypeClass));
        aspectRepository.register(ApiEnrichment.class, new ApiEnrichmentExecutor<CepState, Msg>(apiClient, cepStateTypeClass, msgTypeClass));
        this.executor = aspectRepository.build();
    }

    @Override
    public CepEvent execute(String key, EnrichmentWithDependencies aspect, ValueEnvelope<CepState, Msg> input) {
        return executor.execute(key, aspect, input);
    }

}
