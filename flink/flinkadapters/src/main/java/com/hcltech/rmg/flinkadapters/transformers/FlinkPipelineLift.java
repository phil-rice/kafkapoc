package com.hcltech.rmg.flinkadapters.transformers;

import com.hcltech.rmg.cepstate.worklease.ITokenGenerator;
import com.hcltech.rmg.cepstate.worklease.WorkLease;
import com.hcltech.rmg.common.ITimeService;
import com.hcltech.rmg.flinkadapters.envelopes.ErrorEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.RetryEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.ValueEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.ValueRetryErrorEnvelope;
import com.hcltech.rmg.interfaces.pipeline.IAsyncPipeline;
import com.hcltech.rmg.interfaces.pipeline.IPipeline;
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

    /**
     * Partition-local pipeline with async "lanes" (no keyBy, no shuffle).
     *
     * @param workLease       optional WorkLease to ensure within domain ordering.
     * @param input           stream of ValueEnvelope&lt;From&gt; (parallelism inherited from source, e.g., partitions)
     * @param repository      fully-qualified class name for the IPipelineRepository implementation
     * @param retriesTag      side-output tag for retries
     * @param errorsTag       side-output tag for errors
     * @param lanes           async capacity per subtask (your “lanes per partition”)
     * @param orderedAsync    true = orderedWait (may HOL-block), false = unorderedWait (preferred)
     * @param timeoutBufferMs extra slack added to operator timeout (backstop > internal CF timeout)
     */
    public static <From, To> SingleOutputStreamOperator<ValueEnvelope<To>> lift(
            DataStream<ValueEnvelope<From>> input,
            String repository,
            OutputTag<RetryEnvelope<Object>> retriesTag,
            OutputTag<ErrorEnvelope<Object>> errorsTag,
            int lanes,
            int maxRetries,
            boolean orderedAsync,
            int timeoutBufferMs
    ) {
        ITimeService timeService = ITimeService.real;
        ITokenGenerator tokenGenerator = ITokenGenerator.generator();
        WorkLease worklease = WorkLease.memory( tokenGenerator);
        // 0) Load pipeline definition once on JM
        PipelineDetails<Object, Object> details = IPipelineRepository.load(repository).pipelineDetails();

        // 1) Widen to common super type used by adapters; keep inherited parallelism (no rescale)
        final TypeInformation<ValueRetryErrorEnvelope> vreType =
                TypeExtractor.getForClass(ValueRetryErrorEnvelope.class);

        SingleOutputStreamOperator<ValueRetryErrorEnvelope> current = input
                .map(v -> (ValueRetryErrorEnvelope) v)
                .returns(vreType)
                .name("partition-start");

        // 2) Run each stage; keep inherited parallelism; add capacity on async
        for (Map.Entry<String, PipelineStageDetails<?, ?>> e : details.stages().entrySet()) {
            final String stageName = e.getKey();
            final long stageTimeoutMs = e.getValue().timeOutMs();
            final long operatorTimeoutMs = stageTimeoutMs + Math.max(timeoutBufferMs, 0);

            IPipeline<?, ?> pipeline = e.getValue().pipeline();
            if (pipeline instanceof ISyncPipeline<?, ?>) {
                current = current
                        .flatMap(new SyncOutcomeAdapter<>(stageName, repository))
                        .name(stageName).uid(stageName)
                        .returns(vreType);
            } else if (pipeline instanceof IAsyncPipeline<?, ?> && orderedAsync) {
                current = AsyncDataStream.orderedWait(
                                current,
                                new AsyncOutcomeAdapter<>(stageName, repository),
                                operatorTimeoutMs, TimeUnit.MILLISECONDS,
                                lanes)
                        .name(stageName).uid(stageName)
                        .returns(vreType);
            } else if (pipeline instanceof IAsyncPipeline<?, ?> && !orderedAsync) {

                current = AsyncDataStream.unorderedWait(
                                current,
                                new AsyncOutcomeAdapter<>(stageName, repository),
                                operatorTimeoutMs, TimeUnit.MILLISECONDS,
                                lanes) // capacity
                        .name(stageName).uid(stageName)
                        .returns(vreType);
            } else {
                throw new IllegalArgumentException(
                        "Pipeline stage '" + stageName + "' must be ISyncPipeline or IAsyncPipeline, but was "
                                + pipeline.getClass().getName());
            }
        }

        // 3) Final split; still inherit parallelism (no .setParallelism)
        SingleOutputStreamOperator<ValueEnvelope<Object>> result = current
                .process(new SplitToEnvelopes<Object>(retriesTag, errorsTag))
                .name("split-final").uid("split-final");

        @SuppressWarnings("unchecked")
        SingleOutputStreamOperator<ValueEnvelope<To>> typed =
                (SingleOutputStreamOperator<ValueEnvelope<To>>) (DataStream<?>) result;
        return typed;
    }

    private FlinkPipelineLift() {
    }
}
