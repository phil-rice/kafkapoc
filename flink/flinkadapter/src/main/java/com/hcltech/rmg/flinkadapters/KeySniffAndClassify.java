// KeySniff.java  ->  KeySniffAndClassify.java
package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.appcontainer.impl.AppContainerFactory;
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

/**
 * Minimal hot-path operator:
 * - extracts the domain key via streaming XML (StAX)
 * - (optionally) computes lane index (hash(key) % lanes) – if you need it later
 * - emits (key, raw) on success
 * - side-outputs ErrorEnvelope with ValueEnvelope(null, raw) on failure
 */
public final class KeySniffAndClassify
        extends ProcessFunction<RawMessage, Tuple2<String, RawMessage>> {

    private static final String STAGE = "key-sniff";

    private final String containerId;
    private List<String> keyPath;
    private final OutputTag<ErrorEnvelope<?>> errorsOut;
    private final int lanes;

    private transient KeyExtractor extractor;

    public KeySniffAndClassify(String containerId,
                               OutputTag<ErrorEnvelope<?>> errorsOut,
                               int lanes) {
        this.containerId = containerId;
        this.errorsOut = errorsOut;
        this.lanes = lanes;
    }

    @Override
    public void open(OpenContext ctx) {
        var container = AppContainerFactory.resolve(containerId).valueOrThrow();
        this.extractor = container.xml();
        this.keyPath = container.keyPath();
        if (keyPath == null || keyPath.isEmpty()) {
            throw new IllegalStateException("AppContainer.keyPath() is null/empty for id=" + containerId);
        }
    }

    @Override
    public void processElement(RawMessage raw, Context ctx, Collector<Tuple2<String, RawMessage>> out) {
        ErrorsOr<String> eo = extractor.extractId(raw.rawValue(), keyPath);

        if (eo.isValue()) {
            out.collect(Tuple2.of(eo.valueOrThrow(), raw));
            return;
        }

        ctx.output(
                errorsOut,
                new ErrorEnvelope<>(
                        new ValueEnvelope<>(null, raw),
                        STAGE,
                        eo.errorsOrThrow()
                )
        );
    }
}
