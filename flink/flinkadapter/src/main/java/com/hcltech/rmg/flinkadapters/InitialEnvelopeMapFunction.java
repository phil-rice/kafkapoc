package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.appcontainer.impl.AppContainerFactory;
import com.hcltech.rmg.cepstate.CepEventLog;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.InitialEnvelopeFactory;
import com.hcltech.rmg.messages.RawMessage;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.codehaus.stax2.validation.XMLValidationSchema;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * In:  (domainId, raw)
 * Out: Envelope<CepState, Map<String,Object>>
 * <p>
 * All error paths are wrapped as ErrorEnvelope by the factory (via recover()).
 */
public final class InitialEnvelopeMapFunction
        extends RichMapFunction<Tuple2<String, RawMessage>, Envelope<java.util.Map<String, Object>>> {

    private final String containerId;
    private InitialEnvelopeFactory<XMLValidationSchema> factory;

    public InitialEnvelopeMapFunction(String containerId) {
        this.containerId = containerId;
    }

    @Override
    public void open(OpenContext parameters) {
        var container = AppContainerFactory.resolve(containerId).valueOrThrow();
        Supplier<CepEventLog> cepStateSupplier = () -> FlinkCepEventLog.from(getRuntimeContext(), "cepState");
        this.factory = new InitialEnvelopeFactory<XMLValidationSchema>(container.parameterExtractor(),
                container.nameToSchemaMap(),
                cepStateSupplier,
                container.xml(),
                container.eventTypeExtractor(),
                container.domainTypeExtractor(),
                container.keyToConfigMap(),
                container.rootConfig());
    }

    @Override
    public Envelope<java.util.Map<String, Object>> map(Tuple2<String, RawMessage> in) {
        Objects.requireNonNull(factory, "InitialEnvelopeMapFunction not opened");
        String domainId = in.f0;
        RawMessage raw = in.f1;
        return factory.createEnvelopeHeaderAtStart(raw, domainId);
    }
}
