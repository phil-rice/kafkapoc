package com.hcltech.rmg.performance;// SplitEnvelopes.java

import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.ErrorEnvelope;
import com.hcltech.rmg.messages.RetryEnvelope;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class SplitEnvelopes<T> extends ProcessFunction<Envelope<T>, ValueEnvelope<T>> {

    private final OutputTag<ErrorEnvelope<T>> errorsTag;
    private final OutputTag<RetryEnvelope<T>> retriesTag;

    public SplitEnvelopes(OutputTag<ErrorEnvelope<T>> errorsTag, OutputTag<RetryEnvelope<T>> retriesTag) {
        this.errorsTag = errorsTag;
        this.retriesTag = retriesTag;
    }

    @Override
    public void processElement(Envelope<T> e, Context ctx, Collector<ValueEnvelope<T>> out) {
        if (e instanceof ErrorEnvelope<T> ee) {
            ctx.output(errorsTag, ee);
        } else if (e instanceof RetryEnvelope<T> rr) {
            ctx.output(retriesTag, rr);
        } else if (e instanceof ValueEnvelope<T> vv) {
            out.collect(vv);
            return;
        } else {
            // safety net: unknown kind -> mark as error
            var fallback = new ErrorEnvelope<>(e.valueEnvelope(), "splitter", java.util.List.of("Unknown envelope type: " + e.getClass().getName()));
            ctx.output(errorsTag, fallback);
        }
    }
}
