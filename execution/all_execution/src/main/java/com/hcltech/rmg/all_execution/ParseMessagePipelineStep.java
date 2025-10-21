package com.hcltech.rmg.all_execution;

import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.messages.*;
import com.hcltech.rmg.parameters.ParameterExtractor;
import com.hcltech.rmg.xml.XmlTypeClass;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * In:  (domainId, raw)
 * Out: Envelope<CepState, Map<String,Object>>
 * <p>
 * All error paths are wrapped as ErrorEnvelope by the factory (via recover()).
 */
public class ParseMessagePipelineStep<MSC, CepState, Msg, Schema,FlinkRT, FlinkFR,MetricParam> {

    private final XmlTypeClass<Msg, Schema> xmlTypeClass;
    private final IDomainTypeExtractor<Msg> domainTypeExtractor;
    private final IEventTypeExtractor<Msg> eventTypeExtractor;
    private final Schema schema;
    private final ParameterExtractor<Msg> parameterExtractor;
    private final Map<String, Config> keyToConfigMap;

    public ParseMessagePipelineStep(AppContainer<MSC, CepState, Msg, Schema,FlinkRT, FlinkFR,MetricParam> container) {
        this.xmlTypeClass = container.xml();
        this.domainTypeExtractor = container.domainTypeExtractor();
        this.eventTypeExtractor = container.eventTypeExtractor();
        this.schema = container.nameToSchemaMap().get(container.rootConfig().xmlSchemaPath());
        this.parameterExtractor = container.parameterExtractor();
        this.keyToConfigMap = container.keyToConfigMap();
        if (this.schema == null) {
            throw new IllegalArgumentException("Schema not found for: " + container.rootConfig().xmlSchemaPath() + " Legal values: " + container.nameToSchemaMap().keySet());
        }
    }

    public Envelope<CepState, Msg> parse(Envelope<CepState, Msg> in) {
        if (in instanceof ValueEnvelope<CepState, Msg> valueEnvelope) {
            if (in.header().rawMessage().domainId().equals( RawMessage.unknownDomainId))
                return new ErrorEnvelope<>(valueEnvelope, "ParseMessageFunction", List.of("DomainId is unknown"));

            Objects.requireNonNull(xmlTypeClass, "InitialEnvelopeMapFunction not opened");
            RawMessage rawMessage = in.header().rawMessage();
            var message = xmlTypeClass.parseAndValidate(rawMessage.rawValue(), schema);
            var domainType = domainTypeExtractor.extractDomainType(message);
            var eventType = eventTypeExtractor.extractEventType(message);
            if (eventType == null)
                return new ErrorEnvelope<>(valueEnvelope, "ParseMessageFunction", List.of("Event type extraction resulted in null"));
            var parameters = parameterExtractor.parameters(message, eventType, domainType, rawMessage.domainId()).valueOrThrow();
            var behaviorConfig = keyToConfigMap.get(parameters.key()).behaviorConfig();
            var header = new EnvelopeHeader<CepState>(domainType, eventType, rawMessage, parameters, behaviorConfig);
            valueEnvelope.setHeader(header);
            valueEnvelope.setData(message);
            return valueEnvelope;

        }
        return in;
    }
}
