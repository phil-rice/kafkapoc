package com.hcltech.rmg.all_execution;

import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.common.function.Callback;
import com.hcltech.rmg.enrichment.IEnrichmentAspectExecutor;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.ValueEnvelope;

public class EnrichmentPipelineStep<ESC, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricParam> {

    private final IEnrichmentAspectExecutor<CepState, Msg> enrichmentExecutor;

    public EnrichmentPipelineStep(AppContainer<ESC, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricParam> container, String module) {
        this.enrichmentExecutor = container.enrichmentExecutor();
    }

    public void process(int laneId, Envelope<CepState, Msg> envelope, Callback<Envelope<CepState, Msg>> callback) {
        if (envelope instanceof ValueEnvelope<CepState, Msg> valueEnvelope) {
            this.enrichmentExecutor.call(laneId, valueEnvelope, callback);
        }
        callback.success(envelope);
    }

}
