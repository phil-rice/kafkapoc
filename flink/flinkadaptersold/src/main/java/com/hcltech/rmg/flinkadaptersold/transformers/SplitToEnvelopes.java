package com.hcltech.rmg.flinkadaptersold.transformers;

import com.hcltech.rmg.flinkadapters.envelopes.ErrorEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.RetryEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.ValueEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.ValueRetryErrorEnvelope;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;

public final class SplitToEnvelopes<T>
        extends ProcessFunction<ValueRetryErrorEnvelope, ValueEnvelope<T>> {

    private final OutputTag<RetryEnvelope<T>> retriesTag;
    private final OutputTag<ErrorEnvelope<T>> errorsTag;

    public SplitToEnvelopes(OutputTag<RetryEnvelope<T>> retriesTag,
                     OutputTag<ErrorEnvelope<T>> errorsTag) {
        this.retriesTag = retriesTag;
        this.errorsTag = errorsTag;
    }

    @Override
    public void processElement(ValueRetryErrorEnvelope env, Context ctx, Collector<ValueEnvelope<T>> collector) throws Exception {
        if (env instanceof ValueEnvelope<?>) collector.collect((ValueEnvelope<T>) env);
        else if (env instanceof RetryEnvelope<?>) ctx.output(retriesTag, (RetryEnvelope<T>) env);
        else if (env instanceof ErrorEnvelope<?>) ctx.output(errorsTag, (ErrorEnvelope<T>) env);
        else
            throw new IllegalStateException("Envelope must be Value, Retry or Error. Was " + env.getClass().getName());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        SplitToEnvelopes<?> that = (SplitToEnvelopes<?>) o;
        return Objects.equals(retriesTag, that.retriesTag) && Objects.equals(errorsTag, that.errorsTag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(retriesTag, errorsTag);
    }
}