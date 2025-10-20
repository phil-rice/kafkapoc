package com.hcltech.rmg.shared_worker.serialisation;

import com.hcltech.rmg.common.function.LFunction;
import com.hcltech.rmg.common.function.SFunction;
import com.hcltech.rmg.messages.RawMessage;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

public final class ValueRecordSerializer<CepState, Msg>
        implements KafkaRecordSerializationSchema<ValueEnvelope<CepState, Msg>> {

    private final ComposedRecordSerializer<ValueEnvelope<CepState, Msg>> delegate;

    public ValueRecordSerializer(String topic,
                                 SerializationSchema<ValueEnvelope<CepState, Msg>> keySer,
                                 SerializationSchema<ValueEnvelope<CepState, Msg>> valueSer) {

        this.delegate = new ComposedRecordSerializer<>(
                topic,
                keySer,
                valueSer,
                v -> v.header().rawMessage().brokerTimestamp(), // TimestampFn (serializable)
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

    @Override
    public ProducerRecord<byte[], byte[]> serialize(ValueEnvelope<CepState, Msg> v, KafkaSinkContext ctx, Long ts) {
        return delegate.serialize(v, ctx, ts);
    }
}
