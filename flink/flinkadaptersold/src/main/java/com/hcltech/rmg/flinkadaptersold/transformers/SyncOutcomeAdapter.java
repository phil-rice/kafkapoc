package com.hcltech.rmg.flinkadaptersold.transformers;

import com.hcltech.rmg.flinkadapters.envelopes.RetryEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.ValueEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.ValueRetryErrorEnvelope;
import com.hcltech.rmg.interfaces.pipeline.IOneToOneSyncPipeline;
import com.hcltech.rmg.interfaces.repository.IPipelineRepository;
import com.hcltech.rmg.interfaces.repository.PipelineStageDetails;
import com.hcltech.rmg.interfaces.outcome.Outcome;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Objects;

public final class SyncOutcomeAdapter<From, To>
        extends RichFlatMapFunction<ValueRetryErrorEnvelope, ValueRetryErrorEnvelope> {

    private final String stageName;
    private final String repositoryName;

    private transient PipelineStageDetails<From, To> stageDetails;
    private transient IOneToOneSyncPipeline<From, To> pipeline;

    public SyncOutcomeAdapter(String stageName, String repositoryName) {
        this.stageName = stageName;
        this.repositoryName = repositoryName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open(OpenContext parameters) {
        var stages = IPipelineRepository.load(repositoryName).pipelineDetails().stages();
        PipelineStageDetails<From, To> details =
                (PipelineStageDetails<From, To>) stages.get(stageName);
        if (details == null) {
            throw new IllegalArgumentException(
                    "Cannot find details for " + stageName +
                            " in repository " + repositoryName + " legal names are " + stages.keySet());
        }
        this.stageDetails = details;

        if (!(details.pipeline() instanceof IOneToOneSyncPipeline<?, ?> sync)) {
            throw new IllegalArgumentException(
                    "Pipeline for stage " + stageName +
                            " must be IOneTOneSyncPipeline; was " + details.pipeline().getClass().getName());
        }
        this.pipeline = (IOneToOneSyncPipeline<From, To>) sync;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void flatMap(ValueRetryErrorEnvelope in, Collector<ValueRetryErrorEnvelope> out) {
        if (in instanceof ValueEnvelope<?> veRaw) {
            processValueEnvelope((ValueEnvelope<From>) veRaw, out, 0);
        } else if (in instanceof RetryEnvelope<?> reRaw) {
            RetryEnvelope<From> re = (RetryEnvelope<From>) reRaw;
            if (stageName.equals(re.stageName())) {
                processValueEnvelope(re.envelope(), out, re.retryCount() + 1);
            } else {
                out.collect(in); // pass-through for other stages’ retries
            }
        } else {
            out.collect(in); // ErrorEnvelope pass-through
        }
    }

    private void processValueEnvelope(
            ValueEnvelope<From> in,
            Collector<ValueRetryErrorEnvelope> out,
            int retryCountOnFail
    ) {
        Outcome<List<To>> outcome = pipeline.process(in.data()).map(List::of);
        if (outcome.isValue()) {
            for (To v : outcome.valueOrThrow()) {
                // If you have withStage(...), prefer: out.collect(in.withStage(stageName).withData(v));
                out.collect(in.withData(v));
            }
        } else if (outcome.isErrors()) {
            out.collect(in.toErrorEnvelope(stageName, outcome.errorOrThrow()));
        } else if (outcome.isRetry()) {
            out.collect(in.toRetryEnvelope(stageName, retryCountOnFail));
        } else {
            throw new IllegalStateException(
                    "Outcome must be value, retry or error. Was " + outcome.getClass().getName());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SyncOutcomeAdapter<?, ?> that)) return false;
        return Objects.equals(stageName, that.stageName)
                && Objects.equals(repositoryName, that.repositoryName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stageName, repositoryName);
    }
}
