package com.hcltech.rmg.all_execution;

import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.cepstate.CepEventLog;
import com.hcltech.rmg.config.aspect.AspectMap;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.enrichment.IEnrichmentAspectExecutor;
import com.hcltech.rmg.execution.bizlogic.BizLogicExecutor;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.IDomainTypeExtractor;
import com.hcltech.rmg.messages.IEventTypeExtractor;
import com.hcltech.rmg.messages.ValueEnvelope;
import com.hcltech.rmg.xml.XmlTypeClass;

import java.util.Map;
import java.util.function.Supplier;

public class BizLogicPipelineStep<ESC, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricParam> {

    private final IEnrichmentAspectExecutor<CepState, Msg> enrichmentExecutor;
    private final BizLogicExecutor<CepState, Msg> bizLogic;
    private final Map<String, Config> keyToConfigMap;
    private final String module;
    private final XmlTypeClass<Msg, Schema> xmlTypeClass;
    private final IDomainTypeExtractor<Msg> domainTypeExtractor;
    private final IEventTypeExtractor<Msg> eventTypeExtractor;
    private final Schema schema;

    public BizLogicPipelineStep(AppContainer<ESC, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricParam> container, Supplier<CepEventLog> cepStateSupplier, String module) {
        this.module = module;
        this.keyToConfigMap = container.keyToConfigMap();
        this.enrichmentExecutor = container.enrichmentExecutor();
        this.bizLogic = container.bizLogic();
        this.xmlTypeClass = container
                .xml();
        this.domainTypeExtractor = container.domainTypeExtractor();
        this.eventTypeExtractor = container.eventTypeExtractor();
        this.schema = container.nameToSchemaMap().get(container.rootConfig().xmlSchemaPath());
        if (this.schema == null)
            throw new IllegalStateException("Schema not found for: " + container.rootConfig().xmlSchemaPath() + " Legal values: " + container.nameToSchemaMap().keySet());
    }

    public Envelope<CepState, Msg> process(Envelope<CepState, Msg> envelope) {
        if (envelope instanceof ValueEnvelope<CepState, Msg> valueEnvelope) {
            String parameterKey = valueEnvelope.header().parameters().key();
            String fullKey = valueEnvelope.keyForModule(module);
            var config = keyToConfigMap.get(parameterKey);
            AspectMap aspectMap = config.behaviorConfig().events().get(valueEnvelope.header().eventType());
            if (aspectMap == null) return valueEnvelope;
            var bizLogicConfig = aspectMap.bizlogic().get(module);
            Envelope<CepState, Msg> result = bizLogicConfig == null
                    ? valueEnvelope
                    : valueEnvelope.map(ve -> bizLogic.execute(fullKey, bizLogicConfig, ve));
            return result;
        }
        return envelope;
    }

}
