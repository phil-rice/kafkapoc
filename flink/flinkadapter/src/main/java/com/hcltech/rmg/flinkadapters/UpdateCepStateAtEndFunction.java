package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.cepstate.CepEventLog;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.messages.*;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * In:  (domainId, raw)
 * Out: Envelope<CepState, Map<String,Object>>
 * <p>
 * All error paths are wrapped as ErrorEnvelope by the factory (via recover()).
 */
public class UpdateCepStateAtEndFunction<MSC, CepState, Msg, Schema, MetricParam> extends RichMapFunction<Envelope<CepState, Msg>, Envelope<CepState, Msg>> {

    private final AppContainerDefn<MSC, CepState, Msg, Schema, RuntimeContext, MetricParam> appContainerDefn;
    private CepEventLog cepEventLog;

    public UpdateCepStateAtEndFunction(AppContainerDefn<MSC, CepState, Msg, Schema, RuntimeContext, MetricParam> appContainerDefn) {
        this.appContainerDefn = appContainerDefn;
    }

    @Override
    public void open(OpenContext parameters) {
        var container = IAppContainerFactory.resolve(appContainerDefn).valueOrThrow();
        this.cepEventLog = container.eventLogFromRuntimeContext().apply(getRuntimeContext());
    }

    @Override
    public Envelope<CepState, Msg> map(Envelope<CepState, Msg> envelope) {
        Objects.requireNonNull(cepEventLog, "UpdateCepStateAtEndFunction not opened");
        return envelope.map(ve -> {
            cepEventLog.append(ve.cepStateModifications());
            return ve;
        });
    }
}
