package com.hcltech.rmg.shared_worker;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hcltech.rmg.messages.ErrorEnvelope;
import com.hcltech.rmg.messages.RetryEnvelope;
import com.hcltech.rmg.messages.ValueEnvelope;
import com.hcltech.rmg.shared_worker.serialisation.ErrorRecordSerializer;
import com.hcltech.rmg.shared_worker.serialisation.RetryRecordSerializer;
import com.hcltech.rmg.shared_worker.serialisation.ValueRecordSerializer;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Kafka sinks for Value / Error / Retry envelopes.
 * Uses record-level serializers (in serialisation package) to add Kafka headers.
 */
public final class EnvelopeKafkaSinks {

    private EnvelopeKafkaSinks() {}

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ------------------ Builders ------------------

    public static <CepState, Msg> KafkaSink<ValueEnvelope<CepState, Msg>> valueSink(
            String brokers, String topic, Properties producerConfig) {

        return KafkaSink.<ValueEnvelope<CepState, Msg>>builder()
                .setBootstrapServers(brokers)
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(new ValueRecordSerializer<>(
                        topic,
                        new ValueKeySerializer<>(),
                        new ValuePayloadSerializer<>()
                ))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    public static <CepState, Msg> KafkaSink<ErrorEnvelope<CepState, Msg>> errorSink(
            String brokers, String topic, Properties producerConfig) {

        return KafkaSink.<ErrorEnvelope<CepState, Msg>>builder()
                .setBootstrapServers(brokers)
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(new ErrorRecordSerializer<>(
                        topic,
                        new ErrorKeySerializer<>(),
                        new ErrorPayloadSerializer<>()
                ))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    public static <CepState, Msg> KafkaSink<RetryEnvelope<CepState, Msg>> retrySink(
            String brokers, String topic, Properties producerConfig) {

        return KafkaSink.<RetryEnvelope<CepState, Msg>>builder()
                .setBootstrapServers(brokers)
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(new RetryRecordSerializer<>(
                        topic,
                        new RetryKeySerializer<>(),
                        new RetryPayloadSerializer<>()
                ))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    // ------------------ Key serializers ------------------

    public static final class ValueKeySerializer<CepState, Msg>
            implements SerializationSchema<ValueEnvelope<CepState, Msg>> {
        @Override public void open(InitializationContext ctx) {}
        @Override public byte[] serialize(ValueEnvelope<CepState, Msg> v) {
            String k = (v == null || v.header() == null) ? null : v.header().rawMessage().domainId();
            return toBytes(k);
        }
    }

    public static final class ErrorKeySerializer<CepState, Msg>
            implements SerializationSchema<ErrorEnvelope<CepState, Msg>> {
        @Override public void open(InitializationContext ctx) {}
        @Override public byte[] serialize(ErrorEnvelope<CepState, Msg> e) {
            var h = (e == null) ? null : e.valueEnvelope().header();
            String k = (h == null) ? null : h.rawMessage().domainId();
            return toBytes(k);
        }
    }

    public static final class RetryKeySerializer<CepState, Msg>
            implements SerializationSchema<RetryEnvelope<CepState, Msg>> {
        @Override public void open(InitializationContext ctx) {}
        @Override public byte[] serialize(RetryEnvelope<CepState, Msg> r) {
            var h = (r == null) ? null : r.valueEnvelope().header();
            String k = (h == null) ? null : h.rawMessage().domainId();
            return toBytes(k);
        }
    }

    // ------------------ Payload serializers ------------------

    public static final class ValuePayloadSerializer<CepState, Msg>
            implements SerializationSchema<ValueEnvelope<CepState, Msg>> {
        @Override public void open(InitializationContext ctx) {}

        @Override public byte[] serialize(ValueEnvelope<CepState, Msg> v) {
            try (ByteArrayBuilder buf = new ByteArrayBuilder()) {
                JsonGenerator gen = MAPPER.getFactory().createGenerator(buf);
                gen.writeStartObject();
                gen.writeStringField("kind", "value");

                var h = v.header();
                gen.writeStringField("domainType", h.domainType());
                gen.writeStringField("domainId",   h.rawMessage().domainId());
                gen.writeStringField("eventType",  h.eventType());

                gen.writeFieldName("payload");
                // Stream the payload directly; avoids building an intermediate JsonNode
                MAPPER.writeValue(gen, v.data());

                gen.writeEndObject();
                gen.close();
                return buf.toByteArray();
            } catch (Exception ex) {
                return fallback("value");
            }
        }
    }

    public static final class ErrorPayloadSerializer<CepState, Msg>
            implements SerializationSchema<ErrorEnvelope<CepState, Msg>> {
        @Override public void open(InitializationContext ctx) {}

        @Override public byte[] serialize(ErrorEnvelope<CepState, Msg> e) {
            try (ByteArrayBuilder buf = new ByteArrayBuilder()) {
                JsonGenerator gen = MAPPER.getFactory().createGenerator(buf);
                gen.writeStartObject();
                gen.writeStringField("kind", "error");
                gen.writeStringField("stage", e.stageName());

                var h = e.valueEnvelope().header();
                gen.writeStringField("domainType", h.domainType());
                gen.writeStringField("domainId",   h.rawMessage().domainId());
                gen.writeStringField("eventType",  h.eventType());

                gen.writeFieldName("errors");
                MAPPER.writeValue(gen, e.errors());

                gen.writeFieldName("payload");
                MAPPER.writeValue(gen, e.valueEnvelope().data());

                gen.writeEndObject();
                gen.close();
                return buf.toByteArray();
            } catch (Exception ex) {
                return fallback("error");
            }
        }
    }


    public static final class RetryPayloadSerializer<CepState, Msg>
            implements SerializationSchema<RetryEnvelope<CepState, Msg>> {
        @Override public void open(InitializationContext ctx) {}

        @Override public byte[] serialize(RetryEnvelope<CepState, Msg> r) {
            try (ByteArrayBuilder buf = new ByteArrayBuilder()) {
                JsonGenerator gen = MAPPER.getFactory().createGenerator(buf);
                gen.writeStartObject();
                gen.writeStringField("kind", "retry");

                var h = r.valueEnvelope().header();
                gen.writeStringField("domainType", h.domainType());
                gen.writeStringField("domainId",   h.rawMessage().domainId());
                gen.writeStringField("eventType",  h.eventType());
                gen.writeStringField("stageName",  r.stageName() == null ? "" : r.stageName());

                gen.writeFieldName("payload");
                MAPPER.writeValue(gen, r.valueEnvelope().data());

                gen.writeEndObject();
                gen.close();
                return buf.toByteArray();
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
