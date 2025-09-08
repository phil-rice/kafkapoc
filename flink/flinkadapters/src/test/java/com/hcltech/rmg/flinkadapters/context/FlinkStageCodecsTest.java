// src/test/java/com/hcltech/rmg/flinkadapters/context/FlinkStageCodecsTest.java
package com.hcltech.rmg.flinkadapters.context;

import com.hcltech.rmg.common.codec.Codec;
import com.hcltech.rmg.common.codec.JacksonTreeCodec;
import com.hcltech.rmg.flinkadapters.codec.FlinkStageCodecs;
import com.hcltech.rmg.flinkadapters.envelopes.RetryEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.ValueEnvelope;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FlinkStageCodecsTest {

    // --- Simple domain type for tests ---
  record MyEvent(String id, int amount) {}

    @Test
    void valueEnvelope_roundTrips_withSharedMapper() throws Exception {
        // payload codec (exposes ObjectMapper via HasObjectMapper)
        Codec<MyEvent, Map<String, Object>> payload = new JacksonTreeCodec<>(MyEvent.class);

        FlinkStageCodecs<MyEvent> bundle = FlinkStageCodecs.fromPayloadCodec(payload);

        // build envelope
        MyEvent ev = new MyEvent("E-123", 42);
        ValueEnvelope<MyEvent> ve = new ValueEnvelope<>("Order", "id", ev, 1693651200123L);

        // encode -> Map<String,Object>
        String encoded = bundle.valueEnvelope().encode(ve);

        // decode -> ValueEnvelope<MyEvent>
        ValueEnvelope<MyEvent> decoded = bundle.valueEnvelope().decode(encoded);
        assertEquals(ve, decoded);
        assertEquals(ev, decoded.data());
    }

    @Test
    void retryEnvelope_roundTrips_withSharedMapper() throws Exception {
        Codec<MyEvent, Map<String, Object>> payload = Codec.jsonTree(MyEvent.class);
        FlinkStageCodecs<MyEvent> bundle = FlinkStageCodecs.fromPayloadCodec(payload);

        MyEvent ev = new MyEvent("E-999", 7);
        ValueEnvelope<MyEvent> ve = new ValueEnvelope<>("Payment", "domId", ev, 1693651300456L);
        RetryEnvelope<MyEvent> re = new RetryEnvelope<>(ve, "stageName", 3);

        String encoded = bundle.retryEnvelope().encode(re);

        RetryEnvelope<MyEvent> decoded = bundle.retryEnvelope().decode(encoded);
        assertEquals(re, decoded);
        assertEquals(ve, decoded.envelope());
        assertEquals(ev, decoded.envelope().data());
    }

}
