package com.hcltech.rmg.all_execution;

import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.common.function.CallWithCallback;
import com.hcltech.rmg.common.function.Callback;
import com.hcltech.rmg.enrichment.IEnrichmentAspectExecutor;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.ValueEnvelope;

public final class EnrichmentPipelineStep<ESC, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricParam>
        implements CallWithCallback<Envelope<CepState, Msg>, Envelope<CepState, Msg>> {

    private final IEnrichmentAspectExecutor<CepState, Msg> enrichmentExecutor;

    public EnrichmentPipelineStep(
            AppContainer<ESC, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricParam> container,
            @SuppressWarnings("unused") String module // not needed anymore; kept only to avoid call-site churn
    ) {
        this.enrichmentExecutor = container.enrichmentExecutor();
    }

    @Override
    public void call(Envelope<CepState, Msg> envelope, Callback<? super Envelope<CepState, Msg>> cb) {
        if (!(envelope instanceof ValueEnvelope<CepState, Msg> ve)) {
            cb.success(envelope); // pass-through
            return;
        }
        try {
            // Async-shaped delegate; sync impls may callback inline.
            enrichmentExecutor.call(ve, cb);
        } catch (Throwable t) {
            // Defensive guard in case an executor throws instead of cb.failure(...)
            cb.failure(t);
        }
    }
}
