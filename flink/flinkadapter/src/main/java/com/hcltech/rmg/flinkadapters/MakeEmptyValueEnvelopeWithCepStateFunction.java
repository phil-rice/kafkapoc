package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.cepstate.CepEventLog;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.messages.*;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * In:  (domainId, raw)
 * Out: Envelope<CepState, Map<String,Object>>
 * <p>
 * All error paths are wrapped as ErrorEnvelope by the factory (via recover()).
 */
public class MakeEmptyValueEnvelopeWithCepStateFunction<MSC, CepState, Msg, Schema, FR,MetricParam> extends RichMapFunction<RawMessage, Envelope<CepState, Msg>> {

    private final AppContainerDefn<MSC, CepState, Msg, Schema, RuntimeContext, FR,MetricParam> appContainerDefn;
    private CepEventLog cepEventLog;
    private CepStateTypeClass<CepState> cepStateTypeClass;

    public MakeEmptyValueEnvelopeWithCepStateFunction(AppContainerDefn<MSC, CepState, Msg, Schema, RuntimeContext,FR, MetricParam> appContainerDefn) {
        this.appContainerDefn = appContainerDefn;
    }

    @Override
    public void open(OpenContext parameters) {
        var container = IAppContainerFactory.resolve(appContainerDefn).valueOrThrow();
        this.cepStateTypeClass = container.cepStateTypeClass();
        this.cepEventLog = container.eventLogFromRuntimeContext().apply(getRuntimeContext());
    }

    @Override
    public Envelope<CepState, Msg> map(RawMessage rawMessage) {
        var header = new EnvelopeHeader<CepState>(IEventTypeExtractor.unknownEventType, null, rawMessage, null, null, Map.of());
        var ve = new ValueEnvelope<CepState, Msg>(header, null, null, new ArrayList<>());
        try {
            Objects.requireNonNull(cepEventLog, "MakeEmptyValueEnvelopeWithCepStateFunction not opened");
            var existing = cepEventLog.getAll();
            if (existing.size()>0) {
                throw new IllegalStateException("Expected empty cep event log but found: " + existing.size() + " events");

            }
            var cepState = cepEventLog.foldAll(cepStateTypeClass, cepStateTypeClass.createEmpty());
            ve.setCepState(cepState);
            return ve;
        } catch (Exception e) {
            return new ErrorEnvelope<CepState, Msg>(ve, "MakeEmptyValueEnvelope", List.of("Exception creating empty envelope: " + e.getMessage()));
        }
    }
}
