package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.appcontainer.impl.AppContainerFactory;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.InitialEnvelopeFactory;
import com.hcltech.rmg.messages.RawMessage;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.codehaus.stax2.validation.XMLValidationSchema;

import java.util.Map;
import java.util.Objects;

/**
 * In:  (domainId, raw)
 * Out: Envelope<CepState, Map<String,Object>>
 * <p>
 * All error paths are wrapped as ErrorEnvelope by the factory (via recover()).
 */
public final class InitialEnvelopeMapFunction<CepState>
        extends RichMapFunction<Tuple2<String, RawMessage>, Envelope<CepState, java.util.Map<String, Object>>> {

    private final String containerId;
    private InitialEnvelopeFactory<XMLValidationSchema, CepState> factory;

    public InitialEnvelopeMapFunction(String containerId) {
        this.containerId = containerId;
    }

    @Override
    public void open(OpenContext parameters) {
        var container = AppContainerFactory.resolve(containerId).valueOrThrow();
        this.factory = new InitialEnvelopeFactory<XMLValidationSchema, CepState>(container.parameterExtractor(),
                container.nameToSchemaMap(),
                container.xml(),
                container.eventTypeExtractor(),
                container.domainTypeExtractor(),
                container.keyToConfigMap(),
                container.rootConfig());
    }

    @Override
    public Envelope<CepState, java.util.Map<String, Object>> map(Tuple2<String, RawMessage> in) {
        Objects.requireNonNull(factory, "InitialEnvelopeMapFunction not opened");
        String domainId = in.f0;
        RawMessage raw = in.f1;
        return factory.createEnvelopeHeaderAtStart(raw, domainId);
    }
}
