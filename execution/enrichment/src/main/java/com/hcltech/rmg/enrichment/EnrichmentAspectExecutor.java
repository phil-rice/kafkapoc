package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.config.enrich.EnrichmentWithDependencies;
import com.hcltech.rmg.execution.aspects.AspectExecutor;
import com.hcltech.rmg.messages.ValueEnvelope;

import java.util.List;
import java.util.Set;

public class EnrichmentAspectExecutor<CepState, Msg> implements IEnrichmentAspectExecutor<CepState, Msg> {


    public EnrichmentAspectExecutor(CepStateTypeClass<CepState> cepStateTypeClass, List<Set<EnrichmentWithDependencies>> generations, AspectExecutor<EnrichmentWithDependencies, ValueEnvelope<CepState, Msg>, CepEvent> evaluator) {
        this.cepStateTypeClass = cepStateTypeClass;
        this.generations = generations;
        this.evaluator = evaluator;
    }

    private final CepStateTypeClass<CepState> cepStateTypeClass;
    private final List<Set<EnrichmentWithDependencies>> generations;
    private final AspectExecutor<EnrichmentWithDependencies, ValueEnvelope<CepState, Msg>, CepEvent> evaluator;

    public ValueEnvelope<CepState, Msg> execute(ValueEnvelope<CepState, Msg> input) {
        var accumulator = input;
        for (Set<EnrichmentWithDependencies> generation : generations)
            for (EnrichmentWithDependencies enricher : generation) {
                var newEvent = evaluator.execute("unused", enricher, accumulator);
                accumulator = accumulator.withNewCepEvent(cepStateTypeClass, newEvent);
            }
        return accumulator;

    }
}
