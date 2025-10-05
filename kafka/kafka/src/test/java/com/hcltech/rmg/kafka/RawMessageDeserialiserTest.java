package com.hcltech.rmg.kafka;

import com.hcltech.rmg.common.ITimeService;
import com.hcltech.rmg.common.uuid.IUuidGenerator;
import com.hcltech.rmg.messages.RawMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

final class RawMessageDeserialiserTest {

    private static final String TOPIC = "test-topic";
    private static final int PARTITION = 7;
    private static final long OFFSET = 4242L;
    private static final long BROKER_TS = 1_725_000_000_000L; // fixed millis
    private static final long FIXED_NOW = 1_726_000_000_000L;  // fake processing time

    private RawMessageDeserialiser newDeser() {
        ITimeService clock = () -> FIXED_NOW;
        IUuidGenerator uuid = IUuidGenerator.defaultGenerator(); // not asserted in most tests
        return new RawMessageDeserialiser(clock, uuid);
    }

    @Test
    @DisplayName("deserialize: headers present → use them verbatim")
    void deserialize_ok_withHeaders() {
        var headers = new RecordHeaders(new Header[] {
                new RecordHeader("traceparent", "00-abc123-xyz456-01".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("tracestate", "foo=bar".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("baggage", "k=v".getBytes(StandardCharsets.UTF_8))
        });

        var rec = buildRecord(TOPIC, PARTITION, OFFSET, BROKER_TS, "key-ignored", "<xml/>", headers);

        var out = new CaptureCollector<RawMessage>();
        newDeser().deserialize(rec, out);

        assertEquals(1, out.items.size());
        var msg = out.items.get(0);

        assertEquals("<xml/>", msg.rawValue());
        assertEquals(BROKER_TS, msg.brokerTimestamp());
        assertEquals(FIXED_NOW, msg.processingTimestamp());
        assertEquals(PARTITION, msg.partition());
        assertEquals(OFFSET, msg.offset());
        assertEquals("00-abc123-xyz456-01", msg.traceparent());
        assertEquals("foo=bar", msg.tracestate());
        assertEquals("k=v", msg.baggage());
    }

    @Test
    @DisplayName("deserialize: missing headers → synthesize a valid traceparent; others null")
    void deserialize_missingHeaders_synthesizesTraceparent() {
        var rec = buildRecord(TOPIC, PARTITION, OFFSET, BROKER_TS, null, "{\"a\":1}", new RecordHeaders());

        // deterministic clock + deterministic UUIDs
        ITimeService clock = () -> FIXED_NOW;
        IUuidGenerator uuid = new IUuidGenerator() {
            private int i = 0;
            @Override public String generate() {
                // first call → traceId, second call → parentId
                return (i++ == 0)
                        ? "123e4567-e89b-12d3-a456-426614174000"
                        : "abcdefab-cdef-abcd-efab-cdefabcdefab";
            }
        };

        var deser = new RawMessageDeserialiser(clock, uuid);
        var out = new CaptureCollector<RawMessage>();
        deser.deserialize(rec, out);

        assertEquals(1, out.items.size());
        var msg = out.items.get(0);

        assertEquals("{\"a\":1}", msg.rawValue());
        assertEquals(BROKER_TS, msg.brokerTimestamp());
        assertEquals(FIXED_NOW, msg.processingTimestamp());
        assertEquals(PARTITION, msg.partition());
        assertEquals(OFFSET, msg.offset());
        assertEquals("00-123e4567e89b12d3a456426614174000-abcdefabcdefabcd-01", msg.traceparent());
        assertNull(msg.tracestate());
        assertNull(msg.baggage());
        assertTrue(
                msg.traceparent().matches("^00-[0-9a-f]{32}-[0-9a-f]{16}-01$"),
                "traceparent must follow W3C format"
        );
    }

    @Test
    @DisplayName("deserialize: empty header values → treated as null then synthesize")
    void empty_header_values_treated_as_null_then_synthesize() {
        var headers = new RecordHeaders(new Header[] {
                new RecordHeader("traceparent", new byte[0]),
                new RecordHeader("tracestate", new byte[0]),
                new RecordHeader("baggage", new byte[0])
        });

        ITimeService clock = () -> FIXED_NOW;
        IUuidGenerator uuid = () -> "11111111-2222-3333-4444-555555555555"; // same for both calls

        var deser = new RawMessageDeserialiser(clock, uuid);
        var rec = buildRecord(TOPIC, PARTITION, OFFSET, BROKER_TS, null, "<p/>", headers);

        var out = new CaptureCollector<RawMessage>();
        deser.deserialize(rec, out);

        assertEquals(1, out.items.size());
        var msg = out.items.get(0);

        assertEquals("<p/>", msg.rawValue());
        assertTrue(msg.traceparent().startsWith("00-"), "traceparent should be synthesized");
        assertNull(msg.tracestate());
        assertNull(msg.baggage());
    }

    @Test
    @DisplayName("deserialize: null value (tombstone) → ignored")
    void deserialize_nullValue_ignored() {
        var rec = buildRecord(TOPIC, PARTITION, OFFSET, BROKER_TS, "k", null, new RecordHeaders());

        var out = new CaptureCollector<RawMessage>();
        newDeser().deserialize(rec, out);

        assertTrue(out.items.isEmpty(), "Null value should be ignored");
    }

    @Test
    @DisplayName("getProducedType: returns TypeInformation.of(RawMessage)")
    void producedType_ok() {
        TypeInformation<RawMessage> ti = newDeser().getProducedType();
        assertEquals(TypeInformation.of(RawMessage.class), ti);
    }

    // ---------- helpers ----------

    /** Build a ConsumerRecord with timestamp + headers. */
    private static ConsumerRecord<byte[], byte[]> buildRecord(
            String topic,
            int partition,
            long offset,
            long timestamp,
            String keyUtf8,
            String valueUtf8,
            RecordHeaders headers
    ) {
        byte[] key = keyUtf8 == null ? null : keyUtf8.getBytes(StandardCharsets.UTF_8);
        byte[] value = valueUtf8 == null ? null : valueUtf8.getBytes(StandardCharsets.UTF_8);

        return new ConsumerRecord<>(
                topic,
                partition,
                offset,
                timestamp,
                TimestampType.CREATE_TIME,
                -1L,                                     // checksum (deprecated)
                key == null ? -1 : key.length,
                value == null ? -1 : value.length,
                key,
                value,
                headers,
                Optional.empty()                         // leaderEpoch
        );
    }

    /** Minimal in-memory Collector. */
    private static final class CaptureCollector<T> implements Collector<T> {
        final List<T> items = new ArrayList<>();
        @Override public void collect(T record) { items.add(record); }
        @Override public void close() {}
    }
}
