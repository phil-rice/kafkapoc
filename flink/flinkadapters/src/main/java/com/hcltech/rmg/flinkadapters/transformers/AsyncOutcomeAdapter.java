package com.hcltech.rmg.flinkadapters.transformers;

import com.hcltech.rmg.flinkadapters.envelopes.RetryEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.ValueEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.ValueRetryErrorEnvelope;
import com.hcltech.rmg.interfaces.repository.IPipelineRepository;
import com.hcltech.rmg.interfaces.repository.PipelineStageDetails;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.List;
import java.util.Objects;

public final class AsyncOutcomeAdapter<From, To> extends RichAsyncFunction<ValueRetryErrorEnvelope, ValueRetryErrorEnvelope> {

    private final String stageName;
    private final String repositoryName;
    private transient PipelineStageDetails<From, To> pipelineStageDetails;

    public AsyncOutcomeAdapter(String stageName, String repositoryName) {
        this.stageName = stageName;
        this.repositoryName = repositoryName;
    }


    @Override
    public void open(OpenContext context) {
        var stages = IPipelineRepository.load(repositoryName).pipelineDetails().stages();
        @SuppressWarnings("unchecked")
        PipelineStageDetails<From, To> details = (PipelineStageDetails<From, To>) stages.get(stageName);
        if (details == null) {
            throw new IllegalArgumentException("Cannot find details for " + stageName +
                    " in repository " + repositoryName + " legal names are " + stages.keySet());
        }
        this.pipelineStageDetails = details;
    }

    @Override
    public void asyncInvoke(ValueRetryErrorEnvelope in,
                            org.apache.flink.streaming.api.functions.async.ResultFuture<ValueRetryErrorEnvelope> rf) {
        if (in instanceof ValueEnvelope<?> ve) {
            processValueEnvelope((ValueEnvelope<From>) ve, rf, 0);
        } else if (in instanceof RetryEnvelope<?> re) {
            processRetryEnvelope((RetryEnvelope<From>) re, rf);
        } else { //an Error envelope...we just push it through
            rf.complete(List.of(in));
        }
    }

    private void processRetryEnvelope(RetryEnvelope<From> in,
                                      org.apache.flink.streaming.api.functions.async.ResultFuture<ValueRetryErrorEnvelope> rf) {
        if (stageName.equals(in.stageName()))
            processValueEnvelope(in.envelope(), rf, in.retryCount() + 1);
        else
            rf.complete(java.util.List.of(in)); // pass through
    }

    private void processValueEnvelope(ValueEnvelope<From> in,
                                      ResultFuture<ValueRetryErrorEnvelope> rf,
                                      int retryCountOnFail) {
        pipelineStageDetails.pipeline().process(in.data()).whenComplete((outcome, err) -> {
            if (outcome.isValue()) {
                rf.complete(com.hcltech.rmg.common.ListComprehensions.map(
                        outcome.valueOrThrow(), in::withData));
            } else if (outcome.isErrors()) {
                rf.complete(java.util.List.of(in.toErrorEnvelope(stageName, outcome.errorOrThrow())));
            } else if (outcome.isRetry()) {
                rf.complete(java.util.List.of(in.toRetryEnvelope(stageName, retryCountOnFail)));
            } else {
                throw new IllegalStateException("Outcome must be value, retry or error. Was " + outcome.getClass().getName());
            }
        });
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        AsyncOutcomeAdapter<?, ?> that = (AsyncOutcomeAdapter<?, ?>) o;
        return Objects.equals(stageName, that.stageName) && Objects.equals(repositoryName, that.repositoryName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stageName, repositoryName);
    }
}
