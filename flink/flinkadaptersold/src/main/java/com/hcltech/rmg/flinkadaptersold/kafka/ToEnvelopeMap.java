package com.hcltech.rmg.flinkadaptersold.kafka;

import com.hcltech.rmg.common.codec.Codec;
import com.hcltech.rmg.flinkadapters.envelopes.ValueEnvelope;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * MapFunction that converts RawKafkaData to ValueEnvelope<T>.
 * <p>
 * Usage:
 * <pre>
 *   DataStream&lt;RawKafkaData&gt; rawStream = ...
 *   DataStream&lt;ValueEnvelope&lt;MyDomain&gt;&gt; envStream = rawStream.map(new ToEnvelopeMap&lt;MyDomain&gt;("MyDomainType", MyDomain.class.getName(), 100) {
 *       &#64;Override
 *       String domainIdExtractor(MyDomain d) { return d.getId(); }
 *
 *       &#64;Override
 *       MyDomain withInitialValues(MyDomain d) { ... } // set defaults if needed
 *   });
 * </pre>
 *
 * @param <T> The domain type inside the envelope
 */
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
        var env = new ValueEnvelope<T>(domainType, domainId, null, rawMsg, lane, r);
        return env;
    }
}
