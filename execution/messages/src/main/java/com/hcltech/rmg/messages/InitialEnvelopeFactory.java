package com.hcltech.rmg.messages;

import com.hcltech.rmg.cepstate.CepEventLog;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.parameters.ParameterExtractor;
import com.hcltech.rmg.xml.XmlTypeClass;
import com.hcltech.rmg.xml.exceptions.XmlValidationException;

import java.util.*;
import java.util.function.Supplier;

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
        this.cepStateTypeClass = Objects.requireNonNull(cepStateTypeClass, "cepStateTypeClass cannot be null");
        this.cepStateSupplier  = Objects.requireNonNull(cepStateSupplier, "cepStateSupplier cannot be null");
        this.nameToSchemaMap   = Objects.requireNonNull(nameToSchemaMap, "nameToSchemaMap cannot be null");
        this.parameterExtractor= Objects.requireNonNull(parameterExtractor, "parameterExtractor cannot be null");
        this.xmlTypeClass      = Objects.requireNonNull(xmlTypeClass, "xmlTypeClass cannot be null");
        this.eventTypeExtractor= Objects.requireNonNull(eventTypeExtractor, "eventTypeExtractor cannot be null");
        this.domainTypeExtractor=Objects.requireNonNull(domainTypeExtractor, "domainTypeExtractor cannot be null");
        this.keyToConfigMap    = Objects.requireNonNull(keyToConfigMap, "keyToConfigMap cannot be null");
        Objects.requireNonNull(rootConfig, "rootConfig cannot be null");

        this.schema = nameToSchemaMap.get(rootConfig.xmlSchemaPath());
        Objects.requireNonNull(this.schema,
                "Schema not found for: " + rootConfig.xmlSchemaPath() + " Legal values: " + nameToSchemaMap.keySet());
    }

    public Envelope<CepState, Msg> createEnvelopeHeaderAtStart(RawMessage rawMessage, String domainId) {
        Objects.requireNonNull(rawMessage, "rawMessage cannot be null");
        Objects.requireNonNull(domainId, "domainId cannot be null");

        // Adapt the new throwing API to ErrorsOr for the rest of the composition
        ErrorsOr<Msg> parsed;
        try {
            Msg message = xmlTypeClass.parseAndValidate(rawMessage.rawValue(), schema);
            parsed = ErrorsOr.lift(message);
        } catch (XmlValidationException e) {
            parsed = ErrorsOr.error("XML validation failed: " + e.getMessage());
        } catch (Exception e) {
            // Safety net for unexpected issues (should be rare)
            parsed = ErrorsOr.error("XML parse/validate unexpected error: " + e.getMessage());
        }

        ErrorsOr<Envelope<CepState, Msg>> result = parsed.flatMap(message -> {
            var domainType = domainTypeExtractor.extractDomainType(message);
            var eventType  = eventTypeExtractor.extractEventType(message);
            if (eventType == null) {
                return ErrorsOr.error("eventType cannot be null. Check eventTypeExtractor function");
            }

            return parameterExtractor.parameters(message, eventType, domainType, domainId)
                    .flatMap(parameters -> {
                        var config = keyToConfigMap.get(parameters.key());
                        if (config == null) {
                            return ErrorsOr.error("missing config for key: " + parameters.key()
                                    + " Legal values: " + keyToConfigMap.keySet());
                        }

                        return cepStateSupplier.get()
                                .safeFoldAll(cepStateTypeClass, cepStateTypeClass.createEmpty())
                                .flatMap(cepState ->
                                        ErrorsOr.lift(
                                                new ValueEnvelope<>(
                                                        new EnvelopeHeader(
                                                                domainType,
                                                                domainId,
                                                                eventType,
                                                                rawMessage,
                                                                parameters,
                                                                config.behaviorConfig(),
                                                                cepState
                                                        ),
                                                        message,
                                                        new ArrayList<>()
                                                )
                                        )
                                );
                    });
        });

        return result.foldError(errors -> {
            var header   = new EnvelopeHeader<CepState>(IEventTypeExtractor.unknownEventType, domainId, null, rawMessage, null, null, null);
            var valueEnv = new ValueEnvelope<CepState, Msg>(header, null, List.of());
            return new ErrorEnvelope<CepState, Msg>(valueEnv, "initial-envelope-factory", errors);
        });
    }
}
