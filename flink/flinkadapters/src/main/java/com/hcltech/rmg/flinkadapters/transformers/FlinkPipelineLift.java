package com.hcltech.rmg.flinkadapters.transformers;

import com.hcltech.rmg.flinkadapters.envelopes.ErrorEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.RetryEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.ValueEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.ValueRetryErrorEnvelope;
import com.hcltech.rmg.interfaces.pipeline.IAsyncPipeline;
import com.hcltech.rmg.interfaces.pipeline.ISyncPipeline;
import com.hcltech.rmg.interfaces.repository.IPipelineRepository;
import com.hcltech.rmg.interfaces.repository.PipelineDetails;
import com.hcltech.rmg.interfaces.repository.PipelineStageDetails;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class FlinkPipelineLift {
    public static <From, To> SingleOutputStreamOperator<ValueEnvelope<To>> lift(
            DataStream<ValueEnvelope<From>> input,
            String repository,
            OutputTag<RetryEnvelope<Object>> retriesTag,
            OutputTag<ErrorEnvelope<Object>> errorsTag
    ) {
        PipelineDetails<Object, Object> details = IPipelineRepository.load(repository).pipelineDetails();

        // 1) Widen the stream to the common supertype the async fn expects
        TypeInformation<ValueRetryErrorEnvelope> vreType =
                TypeExtractor.getForClass(ValueRetryErrorEnvelope.class);

        DataStream<ValueRetryErrorEnvelope> current =
                input
                        .map(v -> (ValueRetryErrorEnvelope) v)
                        .returns(vreType)
                        .keyBy(ValueRetryErrorEnvelope::domainId); // assuming all envelopes expose domainId()

        // 2) Run each async stage; hint the OUT type after orderedWait
        for (Map.Entry<String, PipelineStageDetails<?, ?>> e : details.stages().entrySet()) {
            final String stageName = e.getKey();
            final long stageTimeOutMs = e.getValue().timeOutMs();

            if (e.getValue().pipeline() instanceof ISyncPipeline<?, ?>)
                current = current
                        .flatMap(new SyncOutcomeAdapter<>(stageName, repository))
                        .name(stageName).uid(stageName)
                        .returns(vreType);
            else if (e.getValue().pipeline() instanceof IAsyncPipeline<?, ?>)
                current = AsyncDataStream
                        .orderedWait(
                                current,
                                new AsyncOutcomeAdapter<>(stageName, repository),
                                stageTimeOutMs,
                                TimeUnit.MILLISECONDS
                        )
                        .name(stageName)
                        .uid(stageName)
                        .returns(vreType); // <— important: OUT is also ValueRetryErrorEnvelope
            else
                throw new IllegalArgumentException("Pipeline for stage " + stageName + " should be a ISyncPipeline or IAsyncPipeline, was " +
                        e.getValue().pipeline().getClass().getName());
        }

        // 3) Final split
        SingleOutputStreamOperator<ValueEnvelope<Object>> result = current
                .process(new SplitToEnvelopes<Object>(retriesTag, errorsTag))
                .name("split-final")
                .uid("split-final");

        @SuppressWarnings("unchecked")
        var typedResult = (SingleOutputStreamOperator<ValueEnvelope<To>>) (DataStream<?>) result;
        return typedResult;
    }

    private FlinkPipelineLift() {
    }
}
