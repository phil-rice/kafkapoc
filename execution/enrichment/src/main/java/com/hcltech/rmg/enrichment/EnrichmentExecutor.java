package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.config.configs.Configs;
import com.hcltech.rmg.config.enrich.EnrichmentAspect;
import com.hcltech.rmg.config.enrich.FixedEnrichment;
import com.hcltech.rmg.execution.aspects.AspectExecutor;
import com.hcltech.rmg.execution.aspects.AspectExecutorRepository;
import com.hcltech.rmg.messages.ValueEnvelope;

public class EnrichmentExecutor<CepState, Msg> implements AspectExecutor<EnrichmentAspect, ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> {
    private final AspectExecutor<EnrichmentAspect, ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> executor;
    AspectExecutorRepository<EnrichmentAspect, ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> aspectRepository = new AspectExecutorRepository<>();

    public EnrichmentExecutor(Configs configs,  Class<Msg> msgClass) {
        aspectRepository.register(FixedEnrichment.class, new FixedEnrichmentExecutor<>());
        this.executor = aspectRepository.build();
    }

    @Override
    public ValueEnvelope<CepState, Msg> execute(String key, EnrichmentAspect aspect, ValueEnvelope<CepState, Msg> input) {
        return executor.execute(key, aspect, input);
    }

}
