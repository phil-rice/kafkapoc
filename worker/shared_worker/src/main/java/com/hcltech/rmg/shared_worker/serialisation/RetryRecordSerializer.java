package com.hcltech.rmg.shared_worker.serialisation;

import com.hcltech.rmg.common.function.LFunction;
import com.hcltech.rmg.common.function.SFunction;
import com.hcltech.rmg.messages.RawMessage;
import com.hcltech.rmg.messages.RetryEnvelope;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

public final class RetryRecordSerializer<CepState, Msg>
        implements KafkaRecordSerializationSchema<RetryEnvelope<CepState, Msg>> {

    private final ComposedRecordSerializer<RetryEnvelope<CepState, Msg>> delegate;

    public RetryRecordSerializer(String topic,
                                 SerializationSchema<RetryEnvelope<CepState, Msg>> keySer,
                                 SerializationSchema<RetryEnvelope<CepState, Msg>> valueSer) {

        this.delegate = new ComposedRecordSerializer<>(
                topic,
                keySer,
                valueSer,
                r -> r.valueEnvelope().header().rawMessage().brokerTimestamp(),
                List.of(
                        HeaderPopulators.w3cTrace((SFunction<RetryEnvelope<CepState, Msg>, RawMessage>)
                                r -> r.valueEnvelope().header().rawMessage()),
                        HeaderPopulators.domain(
                                (SFunction<RetryEnvelope<CepState, Msg>, String>) r -> r.valueEnvelope().header().rawMessage().domainId(),
                                (LFunction<RetryEnvelope<CepState, Msg>>) e -> e.valueEnvelope().getFullCepStateSize(),
                                (LFunction<RetryEnvelope<CepState, Msg>>) r -> r.valueEnvelope().getDurationNanos()
                        ),
                        HeaderPopulators.header("stageName",
                                (SFunction<RetryEnvelope<CepState, Msg>, String>) RetryEnvelope::stageName)
                )
        );
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(RetryEnvelope<CepState, Msg> r, KafkaSinkContext ctx, Long ts) {
        return delegate.serialize(r, ctx, ts);
    }
}
