package com.hcltech.rmg.performance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hcltech.rmg.messages.ErrorEnvelope;
import com.hcltech.rmg.messages.RetryEnvelope;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka sinks for Value / Error / Retry envelopes.
 * NOTE: Uses named SerializationSchema classes (no lambdas) so Flink's type extractor is happy.
 */
public final class EnvelopeKafkaSinks {

    private EnvelopeKafkaSinks() {
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ------------------ Builders ------------------

    public static KafkaSink<ValueEnvelope<Map<String, Object>>> valueSink(
            String brokers, String topic, Properties producerConfig) {

        return KafkaSink.<ValueEnvelope<Map<String, Object>>>builder()
                .setBootstrapServers(brokers)
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<ValueEnvelope<Map<String, Object>>>builder()
                                .setTopic(topic)
                                .setKeySerializationSchema(new ValueKeySerializer())
                                .setValueSerializationSchema(new ValuePayloadSerializer())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    public static KafkaSink<ErrorEnvelope<Map<String, Object>>> errorSink(
            String brokers, String topic, Properties producerConfig) {

        return KafkaSink.<ErrorEnvelope<Map<String, Object>>>builder()
                .setBootstrapServers(brokers)
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<ErrorEnvelope<Map<String, Object>>>builder()
                                .setTopic(topic)
                                .setKeySerializationSchema(new ErrorKeySerializer())
                                .setValueSerializationSchema(new ErrorPayloadSerializer())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    public static KafkaSink<RetryEnvelope<Map<String, Object>>> retrySink(
            String brokers, String topic, Properties producerConfig) {

        return KafkaSink.<RetryEnvelope<Map<String, Object>>>builder()
                .setBootstrapServers(brokers)
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<RetryEnvelope<Map<String, Object>>>builder()
                                .setTopic(topic)
                                .setKeySerializationSchema(new RetryKeySerializer())
                                .setValueSerializationSchema(new RetryPayloadSerializer())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    // ------------------ Key serializers ------------------

    public static final class ValueKeySerializer
            implements SerializationSchema<ValueEnvelope<Map<String, Object>>> {
        @Override
        public void open(InitializationContext ctx) {
        }

        @Override
        public byte[] serialize(ValueEnvelope<Map<String, Object>> v) {
            String k = (v == null || v.header() == null) ? null : v.header().domainId();
            return toBytes(k);
        }
    }

    public static final class ErrorKeySerializer
            implements SerializationSchema<ErrorEnvelope<Map<String, Object>>> {
        @Override
        public void open(InitializationContext ctx) {
        }

        @Override
        public byte[] serialize(ErrorEnvelope<Map<String, Object>> e) {
            var h = (e == null) ? null : e.valueEnvelope().header();
            String k = (h == null) ? null : h.domainId();
            return toBytes(k);
        }
    }

    public static final class RetryKeySerializer
            implements SerializationSchema<RetryEnvelope<Map<String, Object>>> {
        @Override
        public void open(InitializationContext ctx) {
        }

        @Override
        public byte[] serialize(RetryEnvelope<Map<String, Object>> r) {
            var h = (r == null) ? null : r.valueEnvelope().header();
            String k = (h == null) ? null : h.domainId();
            return toBytes(k);
        }
    }

    // ------------------ Payload serializers ------------------

    public static final class ValuePayloadSerializer
            implements SerializationSchema<ValueEnvelope<Map<String, Object>>> {
        @Override
        public void open(InitializationContext ctx) {
        }

        @Override
        public byte[] serialize(ValueEnvelope<Map<String, Object>> v) {
            try {
                ObjectNode root = MAPPER.createObjectNode();
                root.put("kind", "value");
                var h = v.header();
                root.put("domainType", h.domainType());
                root.put("domainId", h.domainId());
                root.put("eventType", h.eventType());
                root.set("payload", MAPPER.valueToTree(v.data()));
                return MAPPER.writeValueAsBytes(root);
            } catch (Exception ex) {
                return fallback("value");
            }
        }
    }

    public static final class ErrorPayloadSerializer
            implements SerializationSchema<ErrorEnvelope<Map<String, Object>>> {
        @Override
        public void open(InitializationContext ctx) {
        }

        @Override
        public byte[] serialize(ErrorEnvelope<Map<String, Object>> e) {
            try {
                ObjectNode root = MAPPER.createObjectNode();
                root.put("kind", "error");
                root.put("stage", e.stageName());
                var h = e.valueEnvelope().header();
                root.put("domainType", h.domainType());
                root.put("domainId", h.domainId());
                root.put("eventType", h.eventType());
                root.set("errors", MAPPER.valueToTree(e.errors()));
                root.set("payload", MAPPER.valueToTree(e.valueEnvelope().data()));
                return MAPPER.writeValueAsBytes(root);
            } catch (Exception ex) {
                return fallback("error");
            }
        }
    }

    public static final class RetryPayloadSerializer
            implements SerializationSchema<RetryEnvelope<Map<String, Object>>> {
        @Override
        public void open(InitializationContext ctx) {
        }

        @Override
        public byte[] serialize(RetryEnvelope<Map<String, Object>> r) {
            try {
                ObjectNode root = MAPPER.createObjectNode();
                root.put("kind", "retry");
                var h = r.valueEnvelope().header();
                root.put("domainType", h.domainType());
                root.put("domainId", h.domainId());
                root.put("eventType", h.eventType());
                root.put("stageName", r.stageName() == null ? "" : r.stageName());
                root.set("payload", MAPPER.valueToTree(r.valueEnvelope().data()));
                return MAPPER.writeValueAsBytes(root);
            } catch (Exception ex) {
                return fallback("retry");
            }
        }
    }

    // ------------------ helpers ------------------

    private static byte[] toBytes(String s) {
        return s == null ? null : s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] fallback(String kind) {
        return ("{\"kind\":\"" + kind + "\",\"error\":\"serialize\"}").getBytes(StandardCharsets.UTF_8);
    }
}
