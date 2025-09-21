package com.hcltech.rmg.flinkadapters.kafka;

import com.hcltech.rmg.common.codec.Codec;
import com.hcltech.rmg.flinkadapters.envelopes.ValueEnvelope;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.function.Function;

public abstract class ToEnvelopeMap<T> extends RichMapFunction<RawKafkaData, ValueEnvelope<T>> implements java.io.Serializable {

    private final String domainType;
    private final int lanes;                 // 100
    private String clazzName;

    // --- non-serializable runtime deps (rebuilt in open) ---
    private transient Codec<T, String> codec;
    private transient Class<T> clazz;

    abstract String domainIdExtractor(T t);

    abstract T withInitialValues(T t);

    public ToEnvelopeMap(String domainType, String clazzName, int lanes) {
        this.domainType = domainType;
        this.clazzName = clazzName;
        this.lanes = lanes;
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        this.clazz = (Class) Class.forName(clazzName);
        this.codec = Codec.clazzCodec(clazz);
    }

    @Override
    public ValueEnvelope<T> map(RawKafkaData r) throws Exception {
        var rawMsg = codec.decode(r.value());     // decode bytes/JSON → domain
        var msg = withInitialValues(rawMsg);      // ensure all fields are set
        var domainId = domainIdExtractor(msg);
        int lane = Math.floorMod(domainId.hashCode(), lanes);
        var env = new ValueEnvelope<T>(domainType, domainId, msg, lane, r);
        return env;
    }
}
