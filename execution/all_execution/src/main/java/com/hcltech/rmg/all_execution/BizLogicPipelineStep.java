package com.hcltech.rmg.all_execution;

import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.common.function.CallWithCallback;
import com.hcltech.rmg.common.function.Callback;
import com.hcltech.rmg.config.aspect.AspectMap;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.execution.bizlogic.BizLogicExecutor;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.ValueEnvelope;

import java.util.Map;
import java.util.function.Supplier;

public final class BizLogicPipelineStep<ESC, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricParam>
        implements CallWithCallback<Envelope<CepState, Msg>, Envelope<CepState, Msg>> {

    private final BizLogicExecutor<CepState, Msg> bizLogic;
    private final Map<String, Config> keyToConfigMap;
    private final String module;
    /** Kept to enforce startup invariant (schema must exist). */
    @SuppressWarnings("unused")
    private final Schema schema;

    public BizLogicPipelineStep(
            AppContainer<ESC, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricParam> container,
            @SuppressWarnings("unused") Supplier<?> cepStateSupplier, // not used here
            String module) {

        this.module = module;
        this.keyToConfigMap = container.keyToConfigMap();
        this.bizLogic = container.bizLogic();

        this.schema = container.nameToSchemaMap().get(container.rootConfig().xmlSchemaPath());
        if (this.schema == null) {
            throw new IllegalStateException(
                    "Schema not found for: " + container.rootConfig().xmlSchemaPath()
                            + " Legal values: " + container.nameToSchemaMap().keySet());
        }
    }

    @Override
    public void call(Envelope<CepState, Msg> envelope,
                     Callback<? super Envelope<CepState, Msg>> cb) {

        if (!(envelope instanceof ValueEnvelope<CepState, Msg> ve)) {
            cb.success(envelope); // pass-through
            return;
        }

        final var header = ve.header();
        final var cfg = keyToConfigMap.get(header.parameters().key());
        if (cfg == null) {
            cb.success(envelope);
            return;
        }

        final var aspectMap = cfg.behaviorConfig().events().get(header.eventType());
        if (aspectMap == null) {
            cb.success(envelope);
            return;
        }

        final var bizLogicAspect = aspectMap.bizlogic().get(module);
        if (bizLogicAspect == null) {
            cb.success(envelope);
            return;
        }

        final String fullKey = ve.keyForModule(module);
        try {
            // Forward the same callback; sync impls may invoke inline.
            bizLogic.call(fullKey, bizLogicAspect, ve, cb);
        } catch (Throwable t) {
            cb.failure(t); // guard against misbehaving sync executors
        }
    }
}
