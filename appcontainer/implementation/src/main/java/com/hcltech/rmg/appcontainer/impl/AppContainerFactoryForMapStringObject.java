// com.hcltech.rmg.appcontainer.impl.AppContainerFactory
package com.hcltech.rmg.appcontainer.impl;

import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.cepstate.MapStringObjectCepStateTypeClass;
import com.hcltech.rmg.common.ITimeService;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.common.uuid.IUuidGenerator;
import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.config.loader.ConfigsBuilder;
import com.hcltech.rmg.config.loader.RootConfigLoader;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import com.hcltech.rmg.messages.IDomainTypeExtractor;
import com.hcltech.rmg.messages.IEventTypeExtractor;
import com.hcltech.rmg.parameters.ParameterExtractor;
import com.hcltech.rmg.parameters.Parameters;
import com.hcltech.rmg.woodstox.WoodstoxXmlForMapStringObjectTypeClass;
import com.hcltech.rmg.xml.XmlTypeClass;
import org.codehaus.stax2.validation.XMLValidationSchema;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public final class AppContainerFactoryForMapStringObject implements IAppContainerFactory<KafkaConfig, Map<String, Object>, Map<String, Object>, XMLValidationSchema> {

    private static final Map<String, ErrorsOr<AppContainer<KafkaConfig, Map<String, Object>, Map<String, Object>, XMLValidationSchema>>> CACHE =
            new ConcurrentHashMap<>();


    public static ErrorsOr<AppContainer<KafkaConfig, Map<String, Object>, Map<String, Object>, XMLValidationSchema>> resolve(String id) {
        requireNonNull(id, "container id must not be null");
        final String norm = id.trim().toLowerCase();
        return CACHE.computeIfAbsent(norm, AppContainerFactoryForMapStringObject::build);
    }

    public static void clearCache() {
        CACHE.clear();
    }

    @Override
    public ErrorsOr<AppContainer<KafkaConfig, Map<String, Object>, Map<String, Object>, XMLValidationSchema>> create(String id) {
        return resolve(id);
    }

    // ---------- envs ----------

    public static final List<String> defaultParameters = List.of("productType", "company");

    private static ErrorsOr<AppContainer<KafkaConfig, Map<String, Object>, Map<String, Object>, XMLValidationSchema>> build(String id) {
        return switch (id) {
            case "prod" -> basic(
                    System::currentTimeMillis,
                    IUuidGenerator.defaultGenerator(),
                    "config/root-prod.json",
                    ParameterExtractor.defaultParameterExtractor(defaultParameters, Map.of(), Map.of(
                            "productType", List.of("msg", "productType"),
                            "company", List.of("msg", "company"))),
                    IEventTypeExtractor.fromPathForMapStringObject(List.of("msg", "eventType")),
                    IDomainTypeExtractor.fixed("parcel"),
                    "config/prod/"
            );
            case "test" -> basic(
                    () -> 1_726_000_000_000L,
                    () -> "11111111-2222-3333-4444-555555555555",
                    "config/root-test.json",
                    ParameterExtractor.defaultParameterExtractor(defaultParameters, Map.of(), Map.of()),
                    IEventTypeExtractor.fromPathForMapStringObject(List.of("eventType")),
                    IDomainTypeExtractor.fixed("parcel"),
                    "config/test/"
            );
            default -> ErrorsOr.error("Unknown container id: " + id);
        };
    }

    // ---------- monadic composition (inlined) ----------

    private static ErrorsOr<AppContainer<KafkaConfig, Map<String, Object>, Map<String, Object>, XMLValidationSchema>> basic(
            ITimeService time,
            IUuidGenerator uuid,
            String rootConfigPath,
            ParameterExtractor<Map<String, Object>> parameterExtractor,
            IEventTypeExtractor<Map<String, Object>> eventTypeExtractor,
            IDomainTypeExtractor<Map<String, Object>> domainTypeExtractor,
            String configResourcePrefix
    ) {
        Objects.requireNonNull(time, "time service must not be null");
        Objects.requireNonNull(uuid, "uuid generator must not be null");
        Objects.requireNonNull(rootConfigPath, "root config path must not be null");

        final CepStateTypeClass<Map<String, Object>> cepStateTypeClass = new MapStringObjectCepStateTypeClass();
        final XmlTypeClass<Map<String, Object>, XMLValidationSchema> xml = new WoodstoxXmlForMapStringObjectTypeClass();
        final List<String> keyPath = List.of("domainId");
        final KafkaConfig eventSourceConfig = KafkaConfig.fromSystemProps();

        // Prefer TCCL for resource loading
        final ClassLoader cl = java.util.Objects.requireNonNullElseGet(
                Thread.currentThread().getContextClassLoader(),
                AppContainerFactoryForMapStringObject.class::getClassLoader
        );


        // RootConfig -> Configs -> SchemaMap -> Container
        return RootConfigLoader.fromClasspath(rootConfigPath).flatMap((RootConfig root) ->
                ConfigsBuilder.buildFromClasspath(
                        root,
                        Parameters::defaultKeyFn,
                        Parameters.defaultResourceFn(configResourcePrefix),
                        cl
                ).flatMap(configs ->
                        XmlTypeClass.loadOptionalSchema(xml, root.xmlSchemaPath()).map(schemaMap ->
                                new AppContainer<>(
                                        time,
                                        uuid,
                                        xml,
                                        cepStateTypeClass,
                                        keyPath,
                                        eventSourceConfig,
                                        root,
                                        parameterExtractor,
                                        schemaMap,
                                        domainTypeExtractor,
                                        eventTypeExtractor,
                                        configs.keyToConfigMap()
                                )
                        )
                )
        );
    }
}
