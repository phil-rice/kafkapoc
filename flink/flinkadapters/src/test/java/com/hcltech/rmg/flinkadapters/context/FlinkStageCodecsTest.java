// src/test/java/com/hcltech/rmg/flinkadapters/context/FlinkStageCodecsTest.java
package com.hcltech.rmg.flinkadapters.context;

import com.hcltech.rmg.common.Codec;
import com.hcltech.rmg.common.JacksonTreeCodec;
import com.hcltech.rmg.flinkadapters.envelopes.RetryEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.ValueEnvelope;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FlinkStageCodecsTest {

    // --- Simple domain type for tests ---
    static final class MyEvent {
        final String id;
        final int amount;

        MyEvent(String id, int amount) {
            this.id = id;
            this.amount = amount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MyEvent that)) return false;
            return amount == that.amount && java.util.Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(id, amount);
        }

        @Override
        public String toString() {
            return "MyEvent{id='%s', amount=%d}".formatted(id, amount);
        }
    }

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
        Codec<MyEvent, Map<String, Object>> payload = new JacksonTreeCodec<>(MyEvent.class);
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

    @Test
    void fromPayloadCodec_throws_ifPayloadCodecDoesNotExposeObjectMapper() {
        // Minimal fake codec that does NOT implement HasObjectMapper
        Codec<MyEvent, Map<String, Object>> badCodec = new Codec<>() {
            @Override
            public Map<String, Object> encode(MyEvent from) {
                return Map.of("id", from.id, "amount", from.amount);
            }

            @Override
            public MyEvent decode(Map<String, Object> to) {
                return new MyEvent((String) to.get("id"), ((Number) to.get("amount")).intValue());
            }
        };

        // Expect the treeUsingParent check (used inside fromPayloadCodec) to fail fast
        assertThrows(IllegalArgumentException.class, () -> FlinkStageCodecs.fromPayloadCodec(badCodec));
    }
}
