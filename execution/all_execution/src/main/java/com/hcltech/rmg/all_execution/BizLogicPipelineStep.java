package com.hcltech.rmg.all_execution;

import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.cepstate.CepEventLog;
import com.hcltech.rmg.common.function.Callback;
import com.hcltech.rmg.config.aspect.AspectMap;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.enrichment.IEnrichmentAspectExecutor;
import com.hcltech.rmg.execution.bizlogic.BizLogicExecutor;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.ValueEnvelope;

import java.util.Map;
import java.util.function.Supplier;

public class BizLogicPipelineStep<ESC, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricParam> {

    private final IEnrichmentAspectExecutor<CepState, Msg> enrichmentExecutor;
    private final BizLogicExecutor<CepState, Msg> bizLogic;
    private final Map<String, Config> keyToConfigMap;
    private final String module;

    public BizLogicPipelineStep(AppContainer<ESC, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricParam> container, Supplier<CepEventLog> cepStateSupplier, String module) {
        this.module = module;
        this.keyToConfigMap = container.keyToConfigMap();
        this.enrichmentExecutor = container.enrichmentExecutor();
        this.bizLogic = container.bizLogic();
    }

    public void process(Envelope<CepState, Msg> envelope, Callback<Envelope<CepState, Msg>> callback) {
        if (envelope instanceof ValueEnvelope<CepState, Msg> valueEnvelope) {
            String parameterKey = valueEnvelope.header().parameters().key();
            String fullKey = valueEnvelope.keyForModule(module);
            var config = keyToConfigMap.get(parameterKey);
            String eventType = valueEnvelope.header().eventType();
            AspectMap aspectMap = config.behaviorConfig().events().get(eventType);
            if (aspectMap != null) {
                var bizLogicConfig = aspectMap.bizlogic().get(module);
                bizLogic.call(fullKey, bizLogicConfig, valueEnvelope, callback);
            }
        }
        callback.success(envelope);
    }

}
