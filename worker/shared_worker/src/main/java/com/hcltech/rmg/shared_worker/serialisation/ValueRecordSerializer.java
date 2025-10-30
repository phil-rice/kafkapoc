package com.hcltech.rmg.shared_worker.serialisation;

import com.hcltech.rmg.common.function.LFunction;
import com.hcltech.rmg.common.function.SFunction;
import com.hcltech.rmg.messages.RawMessage;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.List;

/**
 * Composes key/value serializers and adds headers for Kafka records.
 * Explicitly calls open() on nested SerializationSchemas, since Flink
 * does not automatically invoke them for KafkaRecordSerializationSchema.
 */
public final class ValueRecordSerializer<CepState, Msg>
        implements KafkaRecordSerializationSchema<ValueEnvelope<CepState, Msg>> {

    private final ComposedRecordSerializer<ValueEnvelope<CepState, Msg>> delegate;
    private final SerializationSchema<ValueEnvelope<CepState, Msg>> keySerializer;
    private final SerializationSchema<ValueEnvelope<CepState, Msg>> valueSerializer;
    private final String topic;

    public ValueRecordSerializer(
            String topic,
            SerializationSchema<ValueEnvelope<CepState, Msg>> keySer,
            SerializationSchema<ValueEnvelope<CepState, Msg>> valueSer) {

        this.topic = topic;
        this.keySerializer = keySer;
        this.valueSerializer = valueSer;

        this.delegate = new ComposedRecordSerializer<>(
                topic,
                keySer,
                valueSer,
                v -> v.header().rawMessage().brokerTimestamp(),
                List.of(
                        HeaderPopulators.w3cTrace((SFunction<ValueEnvelope<CepState, Msg>, RawMessage>)
                                v -> v.header().rawMessage()),
                        HeaderPopulators.domain(
                                (SFunction<ValueEnvelope<CepState, Msg>, String>) v -> v.header().rawMessage().domainId(),
                                (LFunction<ValueEnvelope<CepState, Msg>>) e -> e.getFullCepStateSize(),
                                (LFunction<ValueEnvelope<CepState, Msg>>) ValueEnvelope::getDurationNanos
                        )
                )
        );
    }

    /**
     * Flink does not automatically call open() on inner SerializationSchemas,
     * so we must handle it ourselves here.
     */

    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        KafkaRecordSerializationSchema.super.open(context, sinkContext);

        // Initialize key serializer if it supports open()
        if (keySerializer != null) {
            try {
                keySerializer.open(context);
            } catch (IOException e) {
                throw new RuntimeException("Failed to open key serializer", e);
            }
        }

        // Initialize value serializer if it supports open()
        if (valueSerializer != null) {
            try {
                valueSerializer.open(context);
            } catch (IOException e) {
                throw new RuntimeException("Failed to open value serializer", e);
            }
        }
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(ValueEnvelope<CepState, Msg> v, KafkaSinkContext ctx, Long ts) {
        return delegate.serialize(v, ctx, ts);
    }
}
