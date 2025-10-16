package com.hcltech.rmg.performance;// SplitEnvelopes.java

import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.ErrorEnvelope;
import com.hcltech.rmg.messages.RetryEnvelope;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class SplitEnvelopes<CepState,Msg> extends ProcessFunction<Envelope<CepState,Msg>, ValueEnvelope<CepState,Msg>> {

    private final OutputTag<ErrorEnvelope<CepState,Msg>> errorsTag;
    private final OutputTag<RetryEnvelope<CepState,Msg>> retriesTag;

    public SplitEnvelopes(OutputTag<ErrorEnvelope<CepState,Msg>> errorsTag, OutputTag<RetryEnvelope<CepState,Msg>> retriesTag) {
        this.errorsTag = errorsTag;
        this.retriesTag = retriesTag;
    }

    @Override
    public void processElement(Envelope<CepState,Msg> e, Context ctx, Collector<ValueEnvelope<CepState,Msg>> out) {
        if (e instanceof ErrorEnvelope<CepState,Msg> ee) {
            ctx.output(errorsTag, ee);
        } else if (e instanceof RetryEnvelope<CepState,Msg> rr) {
            ctx.output(retriesTag, rr);
        } else if (e instanceof ValueEnvelope<CepState,Msg> vv) {
            out.collect(vv);
            return;
        } else {
            // safety net: unknown kind -> mark as error
            var fallback = new ErrorEnvelope<>(e.valueEnvelope(), "splitter", java.util.List.of("Unknown envelope type: " + e.getClass().getName()));
            ctx.output(errorsTag, fallback);
        }
    }
}
