package com.hcltech.rmg.messages;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.parameters.ParameterExtractor;
import com.hcltech.rmg.xml.XmlTypeClass;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class InitialEnvelopeFactory<Schema, CepState> {
    private final ParameterExtractor parameterExtractor;
    private final Map<String, Schema> nameToSchemaMap;
    private final XmlTypeClass<Schema> xmlTypeClass;
    private final IEventTypeExtractor eventTypeExtractor;
    private final IDomainTypeExtractor domainTypeExtractor;
    private final Map<String, Config> keyToConfigMap;
    private final Schema schema;


    public InitialEnvelopeFactory(ParameterExtractor parameterExtractor,
                                  Map<String, Schema> nameToSchemaMap,
                                  XmlTypeClass<Schema> xmlTypeClass,
                                  IEventTypeExtractor eventTypeExtractor,
                                  IDomainTypeExtractor domainTypeExtractor,
                                  Map<String, Config> keyToConfigMap,
                                  RootConfig rootConfig) {
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

    public ErrorsOr<Envelope<CepState, Map<String, Object>>> createEnvelopeHeaderAtStart(RawMessage rawMessage, String domainId) {
        Objects.requireNonNull(rawMessage, "rawMessage cannot be null");
        Objects.requireNonNull(domainId, "domainId cannot be null");
        ErrorsOr<Envelope<CepState, Map<String, Object>>> result = xmlTypeClass.parseAndValidate(rawMessage.rawValue(), schema).flatMap(
                message -> {
                    var domainType = domainTypeExtractor.extractDomainType(message);
                    var eventType = eventTypeExtractor.extractEventType(message);
                    if (eventType == null)
                        return ErrorsOr.error("eventType cannot be null. Check eventTypeExtractor function");
                    return parameterExtractor.parameters(message, eventType, domainType, domainId).flatMap(parameters -> {
                        var config = keyToConfigMap.get(eventType);
                        return config == null
                                ? ErrorsOr.error("missing config for eventType: " + eventType + " Legal values: " + keyToConfigMap.keySet())
                                : ErrorsOr.lift(new ValueEnvelope<>(new EnvelopeHeader<CepState>(domainType, domainId, eventType, rawMessage, parameters, config.behaviorConfig(), null), message));
                    });
                }
        );
        return result.recover(errors -> {
            var header = new EnvelopeHeader<CepState>(IEventTypeExtractor.unknownEventType, domainId, null, rawMessage, null, null, null);
            var valueEnv = new ValueEnvelope<CepState, Map<String, Object>>(header, Map.of());
            return new ErrorEnvelope<CepState, Map<String, Object>>(valueEnv, "initial-envelope-factory", errors);
        });
    }
}
