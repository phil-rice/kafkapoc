// com.hcltech.rmg.appcontainer.impl.AppContainerFactory
package com.hcltech.rmg.appcontainer.impl;


import com.hcltech.rmg.appcontainer.interfaces.AiDefn;
import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.celimpl.CelRuleBuilders;
import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepEventLog;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.cepstate.MapStringObjectCepStateTypeClass;
import com.hcltech.rmg.common.ITimeService;
import com.hcltech.rmg.common.apiclient.*;
import com.hcltech.rmg.common.async.ExecutorServiceFactory;
import com.hcltech.rmg.common.async.OrderPreservingAsyncExecutorConfig;
import com.hcltech.rmg.common.copy.MapObjectDeepCopy;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.common.uuid.IUuidGenerator;
import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.config.enrich.EnrichmentWithDependencies;
import com.hcltech.rmg.config.loader.ConfigsBuilder;
import com.hcltech.rmg.config.loader.IConfigsBuilder;
import com.hcltech.rmg.config.loader.IRootConfigBuilder;
import com.hcltech.rmg.config.loader.RootConfigLoader;
import com.hcltech.rmg.enrichment.EnrichmentExecutor;
import com.hcltech.rmg.enrichment.IEnrichmentAspectExecutor;
import com.hcltech.rmg.execution.aspects.AspectExecutor;
import com.hcltech.rmg.execution.bizlogic.BizLogicExecutor;
import com.hcltech.rmg.flink_metrics.FlinkMetricsFactory;
import com.hcltech.rmg.flink_metrics.FlinkMetricsParams;
import com.hcltech.rmg.flinkadapters.FlinkCepEventForMapStringObjectLog;
import com.hcltech.rmg.flinkadapters.FlinkCollectorFutureRecordAdapter;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import com.hcltech.rmg.messages.*;
import com.hcltech.rmg.parameters.ParameterExtractor;
import com.hcltech.rmg.parameters.Parameters;
import com.hcltech.rmg.woodstox.WoodstoxXmlForMapStringObjectTypeClass;
import com.hcltech.rmg.woodstox.WoodstoxXmlForMapStringObjectTypeClassNoValidation;
import com.hcltech.rmg.xml.XmlTypeClass;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.Collector;
import org.codehaus.stax2.validation.XMLValidationSchema;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public final class AppContainerFactoryForMapStringObject implements IAppContainerFactory<KafkaConfig, Map<String, Object>, Map<String, Object>, XMLValidationSchema, RuntimeContext, Collector<Envelope<Map<String, Object>, Map<String, Object>>>, FlinkMetricsParams> {

    private static final Map<String, ErrorsOr<AppContainer<KafkaConfig, Map<String, Object>, Map<String, Object>, XMLValidationSchema, RuntimeContext, Collector<Envelope<Map<String, Object>, Map<String, Object>>>, FlinkMetricsParams>>> CACHE =
            new ConcurrentHashMap<>();


    public static ErrorsOr<AppContainer<KafkaConfig, Map<String, Object>, Map<String, Object>, XMLValidationSchema, RuntimeContext, Collector<Envelope<Map<String, Object>, Map<String, Object>>>, FlinkMetricsParams>> resolve(String id) {
        return resolve(id, null);
    }

    public static ErrorsOr<AppContainer<KafkaConfig, Map<String, Object>, Map<String, Object>, XMLValidationSchema, RuntimeContext, Collector<Envelope<Map<String, Object>, Map<String, Object>>>, FlinkMetricsParams>> resolve(String id, AiDefn defnOrNull) {
        requireNonNull(id, "container id must not be null");
        final String norm = id.trim().toLowerCase();
        return CACHE.computeIfAbsent(norm, i -> AppContainerFactoryForMapStringObject.build(i, defnOrNull));
    }

    public static void clearCache() {
        CACHE.clear();
    }

    @Override
    public ErrorsOr<AppContainer<KafkaConfig, Map<String, Object>, Map<String, Object>, XMLValidationSchema, RuntimeContext, Collector<Envelope<Map<String, Object>, Map<String, Object>>>, FlinkMetricsParams>> create(String id, AiDefn defnOrNull) {
        return resolve(id, defnOrNull);
    }

    // ---------- envs ----------

    public static final List<String> defaultParameters = List.of("productType", "company");
    public static final List<String> defaultParametersForProd = List.of("productType");

    public static Envelope<Map<String, Object>, Map<String, Object>> aiMessagePostParse(Envelope<Map<String, Object>, Map<String, Object>> env) {
        if (env instanceof ValueEnvelope<Map<String, Object>, Map<String, Object>> ve) {
            var data = ve.data();
            var io = (Map<String, Object>) data.get("test");
            if (io == null)
                throw new NullPointerException("AI message missing 'test' root");
            var input = (Map<String, Object>) io.get("input");
            if (input == null)
                throw new NullPointerException("AI message missing 'input' field");
            Object output = io.get("output");
            if (output == null)
                throw new NullPointerException("AI message missing 'output' field");
            if (!(output instanceof List)) output = List.of(output);
            ve.setData(input);
            var cargo = new HashMap<>(ve.header().cargo());
            cargo.put(AiFailureEnvelopeFactory.BIZLOGIC_EXPECTED, output);
            return ve.witHeader(ve.header().withCargo(cargo));
        }
        return env;
    }

    private static ErrorsOr<AppContainer<KafkaConfig, Map<String, Object>, Map<String, Object>, XMLValidationSchema, RuntimeContext, Collector<Envelope<Map<String, Object>, Map<String, Object>>>, FlinkMetricsParams>> build(String id, AiDefn aiDefn) {

        return switch (id) {
            case "prod" -> basic(
                    t -> KafkaConfig.fromSystemProps(t, true),
                    id,
                    "mper-input-events",//topic from system properties
                    ITimeService.real,
                    IUuidGenerator.defaultGenerator(),
                    "config/root-prod.json",
                    30_000,
                    RootConfigLoader::fromClasspath,
                    ConfigsBuilder::buildFromClasspath,
                    "noCelCondition",
                    ParameterExtractor.defaultParameterExtractor(defaultParametersForProd, Map.of(), Map.of(
                            "productType", List.of("MPE", "mailPiece", "mailPieceBarcode", "royalMailSegment", "mailTypeCode"))),
                    IEventTypeExtractor.fromPathForMapStringObject(List.of("MPE", "manualScan", "trackedEventCode")),
                    IDomainTypeExtractor.fixed("parcel"),
                    "config/prod/",
                    "/opt/flink-rocksdb-prod",
                    v -> v
            );
            case "dev" -> basic(
                    t -> KafkaConfig.fromSystemProps(t, false),
                    id,
                    null,//topic from system properties
                    ITimeService.real,
                    IUuidGenerator.defaultGenerator(),
                    "config/root-dev.json",
                    30_000,
                    RootConfigLoader::fromClasspath,
                    ConfigsBuilder::buildFromClasspath,
                    "noCelCondition",
                    ParameterExtractor.defaultParameterExtractor(defaultParameters, Map.of(), Map.of(
                            "productType", List.of("msg", "productType"),
                            "company", List.of("msg", "company"))),
                    IEventTypeExtractor.fromPathForMapStringObject(List.of("msg", "eventType")),
                    IDomainTypeExtractor.fixed("parcel"),
                    "config/prod/",
                    "/tmp/flink-rocksdb-prod",
                    v -> v
            );
            case "ai" -> basic(
                    t -> KafkaConfig.fromSystemProps(t, false),
                    id,
                    "input-output-topic",
                    ITimeService.real,
                    IUuidGenerator.defaultGenerator(),
                    "config/root-prod.json",
                    30_000,
                    IRootConfigBuilder.fromValue(aiDefn.rootConfig()),
                    IConfigsBuilder.fromValue(aiDefn.config()),
                    aiDefn.cel(),
                    ParameterExtractor.defaultParameterExtractor(defaultParameters, Map.of(), Map.of(
                            "productType", List.of("msg", "productType"),
                            "company", List.of("msg", "company"))),
                    IEventTypeExtractor.fromPathForMapStringObject(List.of("msg", "eventType")),
                    IDomainTypeExtractor.fixed("parcel"),
                    "config/prod/",
                    "/opt/flink-rocksdb-ai",

                    AppContainerFactoryForMapStringObject::aiMessagePostParse
            );
            case "test" -> basic(
                    t -> KafkaConfig.fromSystemProps(t, false),
                    id,
                    null,//topic from system properties
                    () -> 1_726_000_000_000L,
                    () -> "11111111-2222-3333-4444-555555555555",
                    "config/root-test.json",
                    30_000,
                    RootConfigLoader::fromClasspath,
                    ConfigsBuilder::buildFromClasspath,
                    "noCelCondition",
                    ParameterExtractor.defaultParameterExtractor(defaultParameters, Map.of(), Map.of()),
                    IEventTypeExtractor.fromPathForMapStringObject(List.of("eventType")),
                    IDomainTypeExtractor.fixed("parcel"),
                    "config/test/",
                    "/opt/flink-rocksdb-test",
                    v -> v
            );
            default -> ErrorsOr.error("Unknown container id: " + id);
        };
    }

    // ---------- monadic composition (inlined) ----------

    private static ErrorsOr<AppContainer<KafkaConfig, Map<String, Object>, Map<String, Object>, XMLValidationSchema, RuntimeContext, Collector<Envelope<Map<String, Object>, Map<String, Object>>>, FlinkMetricsParams>> basic(
            Function<String, KafkaConfig> eventSourceConfigFn,
            String env,
            String topicOrNull,
            ITimeService time,
            IUuidGenerator uuid,
            String rootConfigPath,
            int checkpointIntervalMillis,
            IRootConfigBuilder rootConfigBuilder,
            IConfigsBuilder configBuilder,
            String celConditionForAi,
            ParameterExtractor<Map<String, Object>> parameterExtractor,
            IEventTypeExtractor<Map<String, Object>> eventTypeExtractor,
            IDomainTypeExtractor<Map<String, Object>> domainTypeExtractor,
            String configResourcePrefix,
            String rocksDBPath,
            Function<Envelope<Map<String, Object>, Map<String, Object>>, Envelope<Map<String, Object>, Map<String, Object>>> afterParse
    ) {
        Objects.requireNonNull(time, "timeService service must not be null");
        Objects.requireNonNull(uuid, "uuid generator must not be null");
        Objects.requireNonNull(rootConfigPath, "root config path must not be null");

        final FlinkMetricsFactory metricsFactory = new FlinkMetricsFactory(env, "EventProcessor", 100, true);
        final CepStateTypeClass<Map<String, Object>> cepStateTypeClass = new MapStringObjectCepStateTypeClass();
//        final XmlTypeClass<Map<String, Object>, XMLValidationSchema> xml = new WoodstoxXmlForMapStringObjectTypeClass();
        final XmlTypeClass<Map<String, Object>, XMLValidationSchema> xml = new WoodstoxXmlForMapStringObjectTypeClassNoValidation<>();
        final List<String> keyPath = List.of("domainId");
        final KafkaConfig eventSourceConfig = eventSourceConfigFn.apply(topicOrNull);

        // Prefer TCCL for resource loading
        final ClassLoader cl = java.util.Objects.requireNonNullElseGet(
                Thread.currentThread().getContextClassLoader(),
                AppContainerFactoryForMapStringObject.class::getClassLoader
        );

        var msgTypeClass = new MapStringObjectAndListStringMsgTypeClass();
        Function<RuntimeContext, CepEventLog> cepEventLogFunction = rt -> FlinkCepEventForMapStringObjectLog.from(rt, "CepState");


        EnvelopeFailureAdapter<Map<String, Object>, Map<String, Object>> failureAdapter = new EnvelopeFailureAdapter<>("AppContainerForMapStringObject");
        OrderPreservingAsyncExecutorConfig<Envelope<Map<String, Object>, Map<String, Object>>, Envelope<Map<String, Object>, Map<String, Object>>, Collector<Envelope<Map<String, Object>, Map<String, Object>>>> opaeConfig =
                new OrderPreservingAsyncExecutorConfig<>(
                        256,//lane count
                        64,//lane depth
                        512,//max in flight
                        100, //executor threads
                        1_000, //timeout millis
                        new EnvelopeCorrelator<>(),
                        failureAdapter,
                        new FlinkCollectorFutureRecordAdapter<>(failureAdapter),
                        time
                );

        var csvApiClient = new BlockingHttp2ApiClient<String, Map<String, Object>>(HttpClientConfig.withJsonAccept(
                Duration.ofMillis(1000),//connect timeout millis
                Duration.ofMillis(1000),//read timeout millis
                new StringQueryParamCodec<>(new JsonMapDecoder())),
                InsecureHttp2Client::insecureHttp2Client);
        var tokenGenerator = new AzureStorageTokenGenerator();

        // RootConfig -> Configs -> SchemaMap -> Container
        return rootConfigBuilder.create(rootConfigPath).flatMap((RootConfig root) ->
                configBuilder.create(root,
                        Parameters::defaultKeyFn,
                        Parameters.defaultResourceFn(configResourcePrefix),
                        cl
                ).flatMap(configs ->
                        XmlTypeClass.loadOptionalSchema(xml, root.xmlSchemaPath()).flatMap(schemaMap -> {
                            Class<Map<String, Object>> msgClass = (Class) Map.class;
                            AspectExecutor<EnrichmentWithDependencies, ValueEnvelope<Map<String, Object>, Map<String, Object>>, CepEvent> oneEnrichmentExecutor =
                                    new EnrichmentExecutor<>(csvApiClient, cepStateTypeClass, msgTypeClass, tokenGenerator);
                            var bizLogicExecutor = new BizLogicExecutor<Map<String, Object>, Map<String, Object>>(configs, CelRuleBuilders.newRuleBuilder, msgClass);

                            return IEnrichmentAspectExecutor.<Map<String, Object>, Map<String, Object>>create(cepStateTypeClass, configs, oneEnrichmentExecutor).map(
                                    enricher ->

                                            new AppContainer<KafkaConfig, Map<String, Object>, Map<String, Object>, XMLValidationSchema, RuntimeContext, Collector<Envelope<Map<String, Object>, Map<String, Object>>>, FlinkMetricsParams>(
                                                    time,
                                                    uuid,
                                                    ExecutorServiceFactory.fixed(),
                                                    xml,
                                                    afterParse,
                                                    cepStateTypeClass,
                                                    msgTypeClass,
                                                    checkpointIntervalMillis,
                                                    cepEventLogFunction,
                                                    opaeConfig,
                                                    keyPath,
                                                    eventSourceConfig,
                                                    root,
                                                    parameterExtractor,
                                                    schemaMap,
                                                    domainTypeExtractor,
                                                    eventTypeExtractor,
                                                    enricher,
                                                    bizLogicExecutor,
                                                    celConditionForAi,
                                                    CelRuleBuilders.newRuleBuilder,
                                                    new MapObjectDeepCopy(),
                                                    new MapObjectDeepCopy(),
                                                    AiFailureEnvelopeFactory.fromValueEnvelope(),
                                                    rocksDBPath,
                                                    metricsFactory,
                                                    configs.keyToConfigMap()
                                            ));
                        })
                )
        );
    }
}