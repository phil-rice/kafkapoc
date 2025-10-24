package com.hcltech.rmg.all_execution;

import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.enrichment.IEnrichmentAspectExecutor;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.ValueEnvelope;

import java.util.Map;

public class EnrichmentPipelineStep<ESC, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricParam> {

    private final IEnrichmentAspectExecutor<CepState, Msg> enrichmentExecutor;
    private final Map<String, Config> keyToConfigMap;


    public EnrichmentPipelineStep(AppContainer<ESC, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricParam> container, String module) {
        this.keyToConfigMap = container.keyToConfigMap();
        this.enrichmentExecutor = container.enrichmentExecutor();
    }

    public Envelope<CepState, Msg> process(Envelope<CepState, Msg> envelope) {
        if (envelope instanceof ValueEnvelope<CepState, Msg> valueEnvelope) {
            return valueEnvelope.map(enrichmentExecutor::execute);
        }
        return envelope;
    }

}
