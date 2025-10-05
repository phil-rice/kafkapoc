package com.hcltech.rmg.kafka;

import com.hcltech.rmg.appcontainer.impl.AppContainer;
import com.hcltech.rmg.common.ITimeService;
import com.hcltech.rmg.common.uuid.IUuidGenerator;
import com.hcltech.rmg.messages.RawMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

public final class RawMessageDeserialiser implements KafkaRecordDeserializationSchema<RawMessage> {

    // --- serialized ---
    private final String containerId;

    // --- resolved in open() (not serialized) ---
    private transient ITimeService timeService;
    private transient IUuidGenerator uuid;

    /** Prod/IT path: only serialize a tiny string; resolve deps in open(). */
    public RawMessageDeserialiser(String containerId) {
        this.containerId = containerId;
    }

    /** Test-only path: bypass container resolution. */
    public RawMessageDeserialiser(ITimeService timeService, IUuidGenerator uuid) {
        this.containerId = null;
        this.timeService = timeService;
        this.uuid = uuid;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        if (timeService != null && uuid != null) return; // test-only ctor already set them
        var container = AppContainer.resolve(containerId);
        this.timeService = container.time();
        this.uuid = container.uuid();
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> rec, Collector<RawMessage> out) {
        if (rec == null || rec.value() == null) return;

        String value = new String(rec.value(), StandardCharsets.UTF_8);

        String traceparent = headerValue(rec, "traceparent");
        String tracestate  = headerValue(rec, "tracestate");
        String baggage     = headerValue(rec, "baggage");

        // If no traceparent, synthesize a minimal W3C one
        if (traceparent == null || traceparent.isBlank()) {
            traceparent = synthesizeTraceparent(uuid);
        }

        out.collect(new RawMessage(
                value,
                rec.timestamp(),                      // brokerTimestamp
                timeService.currentTimeMillis(),      // processingTimestamp
                rec.partition(),
                rec.offset(),
                traceparent,
                tracestate,
                baggage
        ));
    }

    @Override
    public TypeInformation<RawMessage> getProducedType() {
        return TypeInformation.of(RawMessage.class);
    }

    private static String headerValue(ConsumerRecord<byte[], byte[]> rec, String key) {
        var h = rec.headers().lastHeader(key);
        if (h == null) return null;
        byte[] v = h.value();
        if (v == null || v.length == 0) return null; // treat empty as null
        return new String(v, StandardCharsets.UTF_8);
    }

    /** Build a W3C traceparent: 00-<32 hex traceId>-<16 hex parentId>-01 */
    static String synthesizeTraceparent(IUuidGenerator uuid) {
        String traceId = toHex32(uuid.generate());   // 16-byte (32 hex) trace-id
        String parent  = toHex16(uuid.generate());   // 8-byte (16 hex) parent-id
        return "00-" + traceId + "-" + parent + "-01";
    }

    private static String toHex32(String uuid) {
        String hex = uuid.replace("-", "").toLowerCase();
        if (hex.length() < 32) hex = String.format("%1$-32s", hex).replace(' ', '0');
        return hex.substring(0, 32);
    }

    private static String toHex16(String uuid) {
        String hex = uuid.replace("-", "").toLowerCase();
        if (hex.length() < 16) hex = String.format("%1$-16s", hex).replace(' ', '0');
        return hex.substring(0, 16);
    }
}
