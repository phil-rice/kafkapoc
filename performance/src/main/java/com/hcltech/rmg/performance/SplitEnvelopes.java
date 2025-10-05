package com.hcltech.rmg.performance;// SplitEnvelopes.java

import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.ErrorEnvelope;
import com.hcltech.rmg.messages.RetryEnvelope;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class SplitEnvelopes<CEPState, T> extends ProcessFunction<Envelope<CEPState, T>, ValueEnvelope<CEPState, T>> {

    private final OutputTag<ErrorEnvelope<CEPState, T>> errorsTag;
    private final OutputTag<RetryEnvelope<CEPState, T>> retriesTag;

    public SplitEnvelopes(OutputTag<ErrorEnvelope<CEPState, T>> errorsTag, OutputTag<RetryEnvelope<CEPState, T>> retriesTag) {
        this.errorsTag = errorsTag;
        this.retriesTag = retriesTag;
    }

    @Override
    public void processElement(Envelope<CEPState, T> e, Context ctx, Collector<ValueEnvelope<CEPState, T>> out) {
        if (e instanceof ErrorEnvelope<CEPState, T> ee) {
            ctx.output(errorsTag, ee);
        } else if (e instanceof RetryEnvelope<CEPState, T> rr) {
            ctx.output(retriesTag, rr);
        } else if (e instanceof ValueEnvelope<CEPState, T> vv) {
            out.collect(vv);
            return;
        } else {
            // safety net: unknown kind -> mark as error
            var fallback = new ErrorEnvelope<>(e.valueEnvelope(), "splitter", java.util.List.of("Unknown envelope type: " + e.getClass().getName()));
            ctx.output(errorsTag, fallback);
        }
    }
}
