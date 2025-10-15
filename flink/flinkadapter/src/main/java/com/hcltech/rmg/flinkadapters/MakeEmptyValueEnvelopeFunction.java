package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.appcontainer.interfaces.InitialEnvelopeServices;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.messages.*;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * In:  (domainId, raw)
 * Out: Envelope<CepState, Map<String,Object>>
 * <p>
 * All error paths are wrapped as ErrorEnvelope by the factory (via recover()).
 */
public class MakeEmptyValueEnvelopeFunction<MSC, CepState, Msg, Schema> extends RichMapFunction<RawMessage, Envelope<CepState, Msg>> {

    private final Class<IAppContainerFactory<MSC, CepState, Msg, Schema>> factoryClass;
    private final String containerId;
    private Supplier<FlinkCepEventForMapStringObjectLog> cepStateSupplier;
    private CepStateTypeClass<CepState> cepStateTypeClass;

    public MakeEmptyValueEnvelopeFunction(Class<IAppContainerFactory<MSC, CepState, Msg, Schema>> factoryClass, String containerId) {
        this.factoryClass = factoryClass;
        this.containerId = containerId;
    }

    @Override
    public void open(OpenContext parameters) {
        InitialEnvelopeServices<CepState, Msg, Schema> container = IAppContainerFactory.resolve(factoryClass, containerId).valueOrThrow();
        this.cepStateTypeClass = container.cepStateTypeClass();
        this.cepStateSupplier = () -> FlinkCepEventForMapStringObjectLog.from(getRuntimeContext(), "cepState");

    }

    @Override
    public Envelope<CepState, Msg> map(RawMessage rawMessage) {
        var header = new EnvelopeHeader<CepState>(IEventTypeExtractor.unknownEventType, null, rawMessage, null, null);
        var ve = new ValueEnvelope<CepState, Msg>(header, null, null, new ArrayList<>());
        try {
            Objects.requireNonNull(cepStateSupplier, "MakeEmptyValueEnvelope not opened");
            var cepEventLog = cepStateSupplier.get();
            var cepState = cepEventLog.foldAll(cepStateTypeClass, cepStateTypeClass.createEmpty());
            ve.setCepState(cepState);
            return ve;
        } catch (Exception e) {
            return new ErrorEnvelope<CepState, Msg>(ve, "MakeEmptyValueEnvelope", List.of("Exception creating empty envelope: " + e.getMessage()));
        }
    }
}
