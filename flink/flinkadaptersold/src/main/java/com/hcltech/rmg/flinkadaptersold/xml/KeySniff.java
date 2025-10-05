package com.hcltech.rmg.flinkadaptersold.xml;

import com.hcltech.rmg.appcontainer.impl.AppContainer;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.messages.ErrorEnvelope;
import com.hcltech.rmg.messages.RawMessage;
import com.hcltech.rmg.messages.ValueEnvelope;
import com.hcltech.rmg.xml.KeyExtractor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;

public final class KeySniff
        extends ProcessFunction<RawMessage, Tuple2<String, RawMessage>> {

    private final String containerId;                 // serialized (tiny)
    private final List<String> keyPath;               // serialized (from config)
    private final OutputTag<ErrorEnvelope<Object, RawMessage>> errorsOut;

    private transient KeyExtractor extractor;         // resolved once in open()

    public KeySniff(
            String containerId,
            List<String> keyPath,
            OutputTag<ErrorEnvelope<Object, RawMessage>> errorsOut) {
        this.containerId = containerId;
        this.keyPath = keyPath;
        this.errorsOut = errorsOut;
    }

    @Override
    public void open(OpenContext parameters) {
        this.extractor = AppContainer.resolve(containerId).keyExtractor();
    }

    @Override
    public void processElement(
            RawMessage raw,
            Context ctx,
            Collector<Tuple2<String, RawMessage>> out) {

        ErrorsOr<String> eo = extractor.extractId(raw.rawValue(), keyPath);

        if (eo.isValue()) {
            out.collect(Tuple2.of(eo.valueOrThrow(), raw));
            return;
        }

        // Build a minimal ErrorEnvelope with null header (cheap, serializable)
        ErrorEnvelope<Object, RawMessage> err = new ErrorEnvelope<>(
                new ValueEnvelope<>(null, raw),        // header=null by design here
                "key-sniff",
                eo.errorsOrThrow()
        );
        ctx.output(errorsOut, err);
    }
}
