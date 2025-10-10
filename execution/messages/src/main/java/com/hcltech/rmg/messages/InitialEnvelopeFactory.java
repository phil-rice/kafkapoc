package com.hcltech.rmg.messages;

import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.parameters.ParameterExtractor;
import com.hcltech.rmg.xml.XmlTypeClass;

import java.util.*;
import java.util.function.Supplier;

import com.hcltech.rmg.cepstate.CepEventLog;

public class InitialEnvelopeFactory<CepState, Msg, Schema> {
    private final ParameterExtractor<Msg> parameterExtractor;
    private final CepStateTypeClass<CepState> cepStateTypeClass;
    private final Map<String, Schema> nameToSchemaMap;
    private final XmlTypeClass<Msg, Schema> xmlTypeClass;
    private final IEventTypeExtractor<Msg> eventTypeExtractor;
    private final IDomainTypeExtractor<Msg> domainTypeExtractor;
    private final Map<String, Config> keyToConfigMap;
    private final Schema schema;
    private final Supplier<CepEventLog> cepStateSupplier;


    public InitialEnvelopeFactory(ParameterExtractor<Msg> parameterExtractor,
                                  CepStateTypeClass<CepState> cepStateTypeClass,
                                  Map<String, Schema> nameToSchemaMap,
                                  Supplier<CepEventLog> cepStateSupplier,
                                  XmlTypeClass<Msg, Schema> xmlTypeClass,
                                  IEventTypeExtractor<Msg> eventTypeExtractor,
                                  IDomainTypeExtractor<Msg> domainTypeExtractor,
                                  Map<String, Config> keyToConfigMap,
                                  RootConfig rootConfig) {
        this.cepStateTypeClass = cepStateTypeClass;
        this.cepStateSupplier = cepStateSupplier;
        Objects.requireNonNull(nameToSchemaMap, "nameToSchemaMap cannot be null");
        Objects.requireNonNull(parameterExtractor, "parameterExtractor cannot be null");
        Objects.requireNonNull(xmlTypeClass, "xmlTypeClass cannot be null");
        Objects.requireNonNull(rootConfig, "rootConfig cannot be null");
        Objects.requireNonNull(eventTypeExtractor, "eventTypeExtractor cannot be null");
        Objects.requireNonNull(domainTypeExtractor, "domainTypeExtractor cannot be null");
        Objects.requireNonNull(keyToConfigMap, "keyToConfigMap cannot be null");
        this.keyToConfigMap = keyToConfigMap;
        this.parameterExtractor = parameterExtractor;
        this.nameToSchemaMap = nameToSchemaMap;
        this.xmlTypeClass = xmlTypeClass;
        this.schema = nameToSchemaMap.get(rootConfig.xmlSchemaPath());
        this.eventTypeExtractor = eventTypeExtractor;
        this.domainTypeExtractor = domainTypeExtractor;
        Objects.requireNonNull(schema, "Schema not found for: " + rootConfig.xmlSchemaPath() + " Legal values: " + nameToSchemaMap.keySet());
    }

    public Envelope<CepState, Msg> createEnvelopeHeaderAtStart(RawMessage rawMessage, String domainId) {
        Objects.requireNonNull(rawMessage, "rawMessage cannot be null");
        Objects.requireNonNull(domainId, "domainId cannot be null");

        ErrorsOr<Envelope<CepState, Msg>> result = xmlTypeClass.parseAndValidate(rawMessage.rawValue(), schema)
                .flatMap(message -> {
                    var domainType = domainTypeExtractor.extractDomainType(message);
                    var eventType = eventTypeExtractor.extractEventType(message);
                    if (eventType == null)
                        return ErrorsOr.error("eventType cannot be null. Check eventTypeExtractor function");

                    return parameterExtractor.parameters(message, eventType, domainType, domainId)
                            .flatMap(parameters -> {
                                var config = keyToConfigMap.get(parameters.key());
                                return cepStateSupplier.get().safeFoldAll(cepStateTypeClass, cepStateTypeClass.createEmpty())
                                        .flatMap(cepState ->
                                                config == null
                                                        ? ErrorsOr.error("missing config for key: " + parameters.key()
                                                        + " Legal values: " + keyToConfigMap.keySet())
                                                        : ErrorsOr.lift(
                                                        new ValueEnvelope<>(
                                                                new EnvelopeHeader(
                                                                        domainType,
                                                                        domainId,
                                                                        eventType,
                                                                        rawMessage,
                                                                        parameters,
                                                                        config.behaviorConfig(),
                                                                        cepState),
                                                                message,
                                                                new ArrayList<>()
                                                        )
                                                )
                                        );
                            });
                });

        return result.foldError(errors -> {
            var header = new EnvelopeHeader<CepState>(IEventTypeExtractor.unknownEventType, domainId, null, rawMessage, null, null, null);
            var valueEnv = new ValueEnvelope<CepState, Msg>(header, null, List.of());
            return new ErrorEnvelope<CepState, Msg>(valueEnv, "initial-envelope-factory", errors);
        });
    }
}
