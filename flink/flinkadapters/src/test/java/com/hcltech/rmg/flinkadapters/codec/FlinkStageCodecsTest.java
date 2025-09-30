package com.hcltech.rmg.flinkadapters.codec;

import com.hcltech.rmg.common.codec.Codec;
import com.hcltech.rmg.common.codec.JacksonTreeCodec;
import com.hcltech.rmg.flinkadapters.envelopes.RetryEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.ValueEnvelope;
import com.hcltech.rmg.flinkadapters.kafka.RawKafkaData;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FlinkStageCodecsTest {

    // --- Simple domain type for tests ---
    record MyEvent(String id, int amount) {}

    @Test
    void valueEnvelope_roundTrips_withSharedMapper() throws Exception {
        // Payload codec (ObjectMapper shared across envelope codecs)
        Codec<MyEvent, Map<String, Object>> payload = new JacksonTreeCodec<>(MyEvent.class);

        FlinkStageCodecs<MyEvent> bundle = FlinkStageCodecs.fromPayloadCodec(payload);

        // Build envelope
        MyEvent ev = new MyEvent("E-123", 42);
        RawKafkaData rkd = new RawKafkaData(
                "{\"id\":\"E-123\",\"amount\":42}",
                "key-1",
                2,
                1001L,
                1693651200000L
        );

        // NOTE: current ValueEnvelope signature is:
        // (String domainType, String domainId, String token, T data, int lane, RawKafkaData rawKafkaData)
        ValueEnvelope<MyEvent> ve = new ValueEnvelope<>("Order", "E-123", "tok-1", ev, 1, rkd);

        // encode -> String
        String encoded = bundle.valueEnvelope().encode(ve);

        // decode -> ValueEnvelope<MyEvent>
        ValueEnvelope<MyEvent> decoded = bundle.valueEnvelope().decode(encoded);

        assertEquals(ve, decoded, "Envelope should round-trip exactly (including token, lane, and raw data)");
        assertEquals(ev, decoded.data(), "Payload should round-trip");
        assertEquals("Order", decoded.domainType());
        assertEquals("E-123", decoded.domainId());
        assertEquals("tok-1", decoded.token());
        assertEquals(1, decoded.lane());
        assertEquals(rkd, decoded.rawKafkaData());
    }

    @Test
    void retryEnvelope_roundTrips_withSharedMapper() throws Exception {
        // You can also use the static helper if you prefer
        Codec<MyEvent, Map<String, Object>> payload = Codec.jsonTree(MyEvent.class);
        FlinkStageCodecs<MyEvent> bundle = FlinkStageCodecs.fromPayloadCodec(payload);

        MyEvent ev = new MyEvent("E-999", 7);
        RawKafkaData rkd = new RawKafkaData(
                "{\"id\":\"E-999\",\"amount\":7}",
                "key-99",
                3,
                2002L,
                1693651200000L
        );

        ValueEnvelope<MyEvent> ve = new ValueEnvelope<>("Payment", "E-999", "tok-9", ev, 4, rkd);
        RetryEnvelope<MyEvent> re = new RetryEnvelope<>(ve, "stageName", 3);

        String encoded = bundle.retryEnvelope().encode(re);
        RetryEnvelope<MyEvent> decoded = bundle.retryEnvelope().decode(encoded);

        assertEquals(re, decoded, "RetryEnvelope should round-trip exactly");
        assertEquals(ve, decoded.envelope(), "Inner ValueEnvelope should be preserved");
        assertEquals(ev, decoded.envelope().data(), "Payload should round-trip");
        assertEquals("stageName", decoded.stageName());
        assertEquals(3, decoded.retryCount());
        assertEquals("tok-9", decoded.envelope().token());
    }
}
