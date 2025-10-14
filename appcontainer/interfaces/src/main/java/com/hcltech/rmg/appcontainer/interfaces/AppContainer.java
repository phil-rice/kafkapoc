package com.hcltech.rmg.appcontainer.interfaces;

import com.hcltech.rmg.all_execution.AllBizLogic;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.common.ITimeService;
import com.hcltech.rmg.common.uuid.IUuidGenerator;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.config.enrich.EnrichmentAspect;
import com.hcltech.rmg.enrichment.IEnrichmentAspectExecutor;
import com.hcltech.rmg.execution.aspects.AspectExecutor;
import com.hcltech.rmg.messages.IDomainTypeExtractor;
import com.hcltech.rmg.messages.IEventTypeExtractor;
import com.hcltech.rmg.messages.ValueEnvelope;
import com.hcltech.rmg.parameters.ParameterExtractor;
import com.hcltech.rmg.xml.XmlTypeClass;

import java.util.List;
import java.util.Map;

public record AppContainer<EventSourceConfig, CepState, Msg, Schema>(
        // infra
        ITimeService time, IUuidGenerator uuid,

        // XML services
        XmlTypeClass<Msg, Schema> xml,                // keyExtraction + parse + validate

        //CepState
        CepStateTypeClass<CepState> cepStateTypeClass,

        // shared config
        List<String> keyPath, EventSourceConfig eventSourceConfig, RootConfig rootConfig,

        // getting things out of messages
        ParameterExtractor<Msg> parameterExtractor, Map<String, Schema> nameToSchemaMap,
        IDomainTypeExtractor<Msg> domainTypeExtractor, IEventTypeExtractor<Msg> eventTypeExtractor,
        //Execution
        IEnrichmentAspectExecutor<CepState, Msg> enrichmentExecutor,
        AllBizLogic<CepState, Msg> bizLogic,

        //The behaviour of the application. The key is the parameters key.
        Map<String, Config> keyToConfigMap) implements InitialEnvelopeServices<CepState, Msg, Schema> {
}
