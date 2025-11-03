package com.hcltech.rmg.shared_worker;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.common.IEnvGetter;
import com.hcltech.rmg.messages.*;
import com.hcltech.rmg.shared_worker.serialisation.AiFailureRecordSerializer;
import com.hcltech.rmg.shared_worker.serialisation.ErrorRecordSerializer;
import com.hcltech.rmg.shared_worker.serialisation.RetryRecordSerializer;
import com.hcltech.rmg.shared_worker.serialisation.ValueRecordSerializer;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

/**
 * Kafka sinks for Value / Error / Retry envelopes.
 * Uses record-level serializers (in serialisation package) to add Kafka headers.
 */
public final class EnvelopeKafkaSinks {
    private static final Logger log = LoggerFactory.getLogger(EnvelopeKafkaSinks.class);

    private EnvelopeKafkaSinks() {
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ------------------ Builders ------------------

    public static <EventSourceConfig, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricsParam> KafkaSink<ValueEnvelope<CepState, Msg>> valueSink(
            AppContainerDefn<EventSourceConfig, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricsParam> defn, String brokers, String topic, Properties producerConfig) {

        return KafkaSink.<ValueEnvelope<CepState, Msg>>builder()
                .setBootstrapServers(brokers)
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(new ValueRecordSerializer<>(
                        topic,
                        new ValueKeySerializer<>(),
                        new ValuePayloadSerializer<>(defn)
                ))
                .setDeliveryGuarantee(DeliveryGuarantee.NONE)
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

    public static <CepState, Msg> KafkaSink<AiFailureEnvelope<CepState, Msg>> failureSink(
            String brokers, String topic, Properties producerConfig) {

        return KafkaSink.<AiFailureEnvelope<CepState, Msg>>builder()
                .setBootstrapServers(brokers)
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(new AiFailureRecordSerializer<>(
                        topic,
                        new AiFailureKeySerializer<>(),
                        new AiFailurePayloadSerializer<>()
                ))
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

    public static final class AiFailureKeySerializer<CepState, Msg>
            implements SerializationSchema<AiFailureEnvelope<CepState, Msg>> {
        @Override
        public void open(InitializationContext ctx) {
        }

        @Override
        public byte[] serialize(AiFailureEnvelope<CepState, Msg> r) {
            var h = (r == null) ? null : r.valueEnvelope().header();
            String k = (h == null) ? null : h.rawMessage().domainId();
            return toBytes(k);
        }
    }

    // ------------------ Payload serializers ------------------

    public static final class ValuePayloadSerializer<EventSourceConfig, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricsParam>
            implements SerializationSchema<ValueEnvelope<CepState, Msg>> {


        private final AppContainerDefn<EventSourceConfig, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricsParam> defn;
        private MsgTypeClass<Msg, List<String>> msgTypeClass;

        public ValuePayloadSerializer(AppContainerDefn<EventSourceConfig, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricsParam> defn) {
            this.defn = defn;
        }

        @Override
        public void open(InitializationContext ctx) {
            AppContainer<EventSourceConfig, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricsParam> container = IAppContainerFactory.resolve(defn).valueOrThrow();
            this.msgTypeClass = container.msgTypeClass();
        }

        @Override
        public byte[] serialize(ValueEnvelope<CepState, Msg> v) {
            if (msgTypeClass == null)
                throw new IllegalStateException("msgTypeClass is null, so open method of ValuePayloadSerializer was not called");
            try {
                Object mapped = msgTypeClass.getValueFromPath(v.data(), List.of("out"));
                return MAPPER.writeValueAsBytes(mapped);
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
            log.error(e.toString());
            try (ByteArrayBuilder buf = new ByteArrayBuilder()) {
                JsonGenerator gen = MAPPER.getFactory().createGenerator(buf);
                gen.writeStartObject();
                gen.writeStringField("kind", "error");
                gen.writeStringField("stage", e.stageName());

                var h = e.valueEnvelope().header();
                gen.writeStringField("domainType", h.domainType());
                gen.writeStringField("domainId", h.rawMessage().domainId());
                gen.writeStringField("eventType", h.eventType());

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
        @Override
        public void open(InitializationContext ctx) {
        }

        @Override
        public byte[] serialize(RetryEnvelope<CepState, Msg> r) {
            try (ByteArrayBuilder buf = new ByteArrayBuilder()) {
                JsonGenerator gen = MAPPER.getFactory().createGenerator(buf);
                gen.writeStartObject();
                gen.writeStringField("kind", "retry");

                var h = r.valueEnvelope().header();
                gen.writeStringField("domainType", h.domainType());
                gen.writeStringField("domainId", h.rawMessage().domainId());
                gen.writeStringField("eventType", h.eventType());
                gen.writeStringField("stageName", r.stageName() == null ? "" : r.stageName());

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

    public static final class AiFailurePayloadSerializer<CepState, Msg>
            implements SerializationSchema<AiFailureEnvelope<CepState, Msg>> {
        @Override
        public void open(InitializationContext ctx) {
        }

        @Override
        public byte[] serialize(AiFailureEnvelope<CepState, Msg> r) {
            try (ByteArrayBuilder buf = new ByteArrayBuilder()) {
                JsonGenerator gen = MAPPER.getFactory().createGenerator(buf);
                gen.writeStartObject();
                gen.writeStringField("kind", "retry");

                var h = r.valueEnvelope().header();
                gen.writeStringField("domainType", h.domainType());
                gen.writeStringField("domainId", h.rawMessage().domainId());
                gen.writeStringField("eventType", h.eventType());

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
