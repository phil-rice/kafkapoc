package com.hcltech.rmg.shared_worker;

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

    public static <CepState, Msg> KafkaSink<ValueEnvelope<CepState, Msg>> valueSink(
            String brokers, String topic, Properties producerConfig) {

        return KafkaSink.<ValueEnvelope<CepState, Msg>>builder()
                .setBootstrapServers(brokers)
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<ValueEnvelope<CepState, Msg>>builder()
                                .setTopic(topic)
                                .setKeySerializationSchema(new ValueKeySerializer())
                                .setValueSerializationSchema(new ValuePayloadSerializer())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    public static <CepState, Msg> KafkaSink<ErrorEnvelope<CepState, Msg>> errorSink(
            String brokers, String topic, Properties producerConfig) {

        return KafkaSink.<ErrorEnvelope<CepState, Msg>>builder()
                .setBootstrapServers(brokers)
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<ErrorEnvelope<CepState, Msg>>builder()
                                .setTopic(topic)
                                .setKeySerializationSchema(new ErrorKeySerializer())
                                .setValueSerializationSchema(new ErrorPayloadSerializer())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    public static <CepState, Msg> KafkaSink<RetryEnvelope<CepState, Msg>> retrySink(
            String brokers, String topic, Properties producerConfig) {

        return KafkaSink.<RetryEnvelope<CepState, Msg>>builder()
                .setBootstrapServers(brokers)
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<RetryEnvelope<CepState, Msg>>builder()
                                .setTopic(topic)
                                .setKeySerializationSchema(new RetryKeySerializer())
                                .setValueSerializationSchema(new RetryPayloadSerializer())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    // ------------------ Key serializers ------------------

    public static final class ValueKeySerializer<CepState, Msg>
            implements SerializationSchema<ValueEnvelope<CepState, Msg>> {
        @Override
        public void open(InitializationContext ctx) {
        }

        @Override
        public byte[] serialize(ValueEnvelope<CepState, Msg> v) {
            String k = (v == null || v.header() == null) ? null : v.header().rawMessage().domainId();
            return toBytes(k);
        }
    }

    public static final class ErrorKeySerializer<CepState, Msg>
            implements SerializationSchema<ErrorEnvelope<CepState, Msg>> {
        @Override
        public void open(InitializationContext ctx) {
        }

        @Override
        public byte[] serialize(ErrorEnvelope<CepState, Msg> e) {
            var h = (e == null) ? null : e.valueEnvelope().header();
            String k = (h == null) ? null : h.rawMessage().domainId();
            return toBytes(k);
        }
    }

    public static final class RetryKeySerializer<CepState, Msg>
            implements SerializationSchema<RetryEnvelope<CepState, Msg>> {
        @Override
        public void open(InitializationContext ctx) {
        }

        @Override
        public byte[] serialize(RetryEnvelope<CepState, Msg> r) {
            var h = (r == null) ? null : r.valueEnvelope().header();
            String k = (h == null) ? null : h.rawMessage().domainId();
            return toBytes(k);
        }
    }

    // ------------------ Payload serializers ------------------

    public static final class ValuePayloadSerializer<CepState, Msg>
            implements SerializationSchema<ValueEnvelope<CepState, Msg>> {
        @Override
        public void open(InitializationContext ctx) {
        }

        @Override
        public byte[] serialize(ValueEnvelope<CepState, Msg> v) {
            try {
                ObjectNode root = MAPPER.createObjectNode();
                root.put("kind", "value");
                var h = v.header();
                root.put("domainType", h.domainType());
                root.put("domainId", h.rawMessage().domainId());
                root.put("eventType", h.eventType());
                root.set("payload", MAPPER.valueToTree(v.data()));
                return MAPPER.writeValueAsBytes(root);
            } catch (Exception ex) {
                return fallback("value");
            }
        }
    }

    public static final class ErrorPayloadSerializer<CepState, Msg>
            implements SerializationSchema<ErrorEnvelope<CepState, Msg>> {
        @Override
        public void open(InitializationContext ctx) {
        }

        @Override
        public byte[] serialize(ErrorEnvelope<CepState, Msg> e) {
            try {
                ObjectNode root = MAPPER.createObjectNode();
                root.put("kind", "error");
                root.put("stage", e.stageName());
                var h = e.valueEnvelope().header();
                root.put("domainType", h.domainType());
                root.put("domainId", h.rawMessage().domainId());
                root.put("eventType", h.eventType());
                root.set("errors", MAPPER.valueToTree(e.errors()));
                root.set("payload", MAPPER.valueToTree(e.valueEnvelope().data()));
                return MAPPER.writeValueAsBytes(root);
            } catch (Exception ex) {
                return fallback("error");
            }
        }
    }

    public static final class RetryPayloadSerializer<CepState, Msg>
            implements SerializationSchema<RetryEnvelope<CepState, Msg>> {
        @Override
        public void open(InitializationContext ctx) {
        }

        @Override
        public byte[] serialize(RetryEnvelope<CepState, Msg> r) {
            try {
                ObjectNode root = MAPPER.createObjectNode();
                root.put("kind", "retry");
                var h = r.valueEnvelope().header();
                root.put("domainType", h.domainType());
                root.put("domainId", h.rawMessage().domainId());
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
