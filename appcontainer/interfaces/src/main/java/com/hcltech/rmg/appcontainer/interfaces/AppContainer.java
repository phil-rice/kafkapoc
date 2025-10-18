package com.hcltech.rmg.appcontainer.interfaces;

import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.common.ITimeService;
import com.hcltech.rmg.common.uuid.IUuidGenerator;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.enrichment.IEnrichmentAspectExecutor;
import com.hcltech.rmg.execution.bizlogic.BizLogicExecutor;
import com.hcltech.rmg.messages.IDomainTypeExtractor;
import com.hcltech.rmg.messages.IEventTypeExtractor;
import com.hcltech.rmg.metrics.MetricsFactory;
import com.hcltech.rmg.parameters.ParameterExtractor;
import com.hcltech.rmg.xml.XmlTypeClass;

import java.util.List;
import java.util.Map;

public record AppContainer<EventSourceConfig, CepState, Msg, Schema, MetricParam>(
        // infra
        ITimeService timeService, IUuidGenerator uuid,

        // XML services
        XmlTypeClass<Msg, Schema> xml,                // keyExtraction + parse + validate

        //Flink
        CepStateTypeClass<CepState> cepStateTypeClass,
        int checkPointIntervalMillis,

        // shared config
        List<String> keyPath, EventSourceConfig eventSourceConfig, RootConfig rootConfig,

        // getting things out of messages
        ParameterExtractor<Msg> parameterExtractor, Map<String, Schema> nameToSchemaMap,
        IDomainTypeExtractor<Msg> domainTypeExtractor, IEventTypeExtractor<Msg> eventTypeExtractor,
        //Execution
        IEnrichmentAspectExecutor<CepState, Msg> enrichmentExecutor,
        BizLogicExecutor<CepState, Msg> bizLogic,

        //metrics
        MetricsFactory<MetricParam> metricsFactory,

        //The behaviour of the application. The key is the parameters key.
        Map<String, Config> keyToConfigMap) implements InitialEnvelopeServices<CepState, Msg, Schema> {
}
