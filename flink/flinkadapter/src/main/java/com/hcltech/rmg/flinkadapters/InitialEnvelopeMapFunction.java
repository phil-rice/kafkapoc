package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.appcontainer.interfaces.InitialEnvelopeServices;
import com.hcltech.rmg.cepstate.CepEventLog;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.InitialEnvelopeFactory;
import com.hcltech.rmg.messages.RawMessage;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * In:  (domainId, raw)
 * Out: Envelope<CepState, Map<String,Object>>
 * <p>
 * All error paths are wrapped as ErrorEnvelope by the factory (via recover()).
 */
public final class InitialEnvelopeMapFunction<MSC, CepState, Msg, Schema> extends RichMapFunction<Tuple2<String, RawMessage>, Envelope<CepState, Msg>> {

    private final Class<IAppContainerFactory<MSC, CepState, Msg, Schema>> factoryClass;
    private final String containerId;
    private InitialEnvelopeFactory<CepState, Msg, Schema> factory;

    public InitialEnvelopeMapFunction(Class<IAppContainerFactory<MSC, CepState, Msg, Schema>> factoryClass, String containerId) {
        this.factoryClass = factoryClass;
        this.containerId = containerId;
    }

    @Override
    public void open(OpenContext parameters) {
        InitialEnvelopeServices<CepState, Msg, Schema> container = IAppContainerFactory.resolve(factoryClass, containerId).valueOrThrow();
        Supplier<CepEventLog> cepStateSupplier = () -> FlinkCepEventForMapStringObjectLog.from(getRuntimeContext(), "cepState");
        this.factory = new InitialEnvelopeFactory<CepState, Msg, Schema>(
                container.parameterExtractor(),
                container.cepStateTypeClass(),
                container.nameToSchemaMap(),
                cepStateSupplier, container.xml(),
                container.eventTypeExtractor(), container.domainTypeExtractor(), container.keyToConfigMap(), container.rootConfig());
    }

    @Override
    public Envelope<CepState, Msg> map(Tuple2<String, RawMessage> in) {
        Objects.requireNonNull(factory, "InitialEnvelopeMapFunction not opened");
        String domainId = in.f0;
        RawMessage raw = in.f1;
        return factory.createEnvelopeHeaderAtStart(raw, domainId);
    }
}
