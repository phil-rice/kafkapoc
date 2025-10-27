package com.hcltech.rmg.appcontainer.interfaces;

import com.hcltech.rmg.celcore.CelRuleBuilderFactory;
import com.hcltech.rmg.cepstate.CepEventLog;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.common.ITimeService;
import com.hcltech.rmg.common.async.ExecutorServiceFactory;
import com.hcltech.rmg.common.async.OrderPreservingAsyncExecutorConfig;
import com.hcltech.rmg.common.copy.DeepCopy;
import com.hcltech.rmg.common.uuid.IUuidGenerator;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.enrichment.IEnrichmentAspectExecutor;
import com.hcltech.rmg.execution.bizlogic.BizLogicExecutor;
import com.hcltech.rmg.messages.*;
import com.hcltech.rmg.common.metrics.MetricsFactory;
import com.hcltech.rmg.parameters.ParameterExtractor;
import com.hcltech.rmg.xml.XmlTypeClass;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public record AppContainer<EventSourceConfig, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricParam>(
        // infra
        ITimeService timeService,
        IUuidGenerator uuid,
        ExecutorServiceFactory executorServiceFactory,
        // XML services
        XmlTypeClass<Msg, Schema> xml,                // keyExtraction + parse + validate
        Function<Envelope<CepState, Msg>, Envelope<CepState, Msg>> afterParse,
//adjusts the message after parsing. Could be validation. In the case of the AI pipeline we extract input and put the expected output into cargo


        //Flink
        CepStateTypeClass<CepState> cepStateTypeClass,
        int checkPointIntervalMillis,
        Function<FlinkRT, CepEventLog> eventLogFromRuntimeContext,
        OrderPreservingAsyncExecutorConfig<Envelope<CepState, Msg>, Envelope<CepState, Msg>, FlinkFR> asyncCfg,

        // shared config
        List<String> keyPath,
        EventSourceConfig eventSourceConfig,
        RootConfig rootConfig,

        // getting things out of messages
        ParameterExtractor<Msg> parameterExtractor, Map<String, Schema> nameToSchemaMap,
        IDomainTypeExtractor<Msg> domainTypeExtractor, IEventTypeExtractor<Msg> eventTypeExtractor,

        //Execution
        IEnrichmentAspectExecutor<CepState, Msg> enrichmentExecutor,
        BizLogicExecutor<CepState, Msg> bizLogic,

        //for ai
        String celConditionForAi,
        CelRuleBuilderFactory celBuilder,
        DeepCopy<Msg> deepCopyMsg,
        DeepCopy<CepState> deepCopyCepState,
        AiFailureEnvelopeFactory<CepState, Msg> aiFailureEnvelopeFactory,

        // RocksDB path for state backend
        String rocksDBPath,

        //metrics
        MetricsFactory<MetricParam> metricsFactory,

        //The behaviour of the application. The key is the parameters key.
        Map<String, Config> keyToConfigMap) {
}
