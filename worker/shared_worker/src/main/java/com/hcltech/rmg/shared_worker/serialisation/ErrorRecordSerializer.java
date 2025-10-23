package com.hcltech.rmg.shared_worker.serialisation;

import com.hcltech.rmg.common.function.LFunction;
import com.hcltech.rmg.common.function.SFunction;
import com.hcltech.rmg.messages.ErrorEnvelope;
import dev.cel.runtime.standard.TimestampFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

public final class ErrorRecordSerializer<CepState, Msg>
        implements KafkaRecordSerializationSchema<ErrorEnvelope<CepState, Msg>> {

    private final ComposedRecordSerializer<ErrorEnvelope<CepState, Msg>> delegate;

    public ErrorRecordSerializer(String topic,
                                 SerializationSchema<ErrorEnvelope<CepState, Msg>> keySer,
                                 SerializationSchema<ErrorEnvelope<CepState, Msg>> valueSer) {

        this.delegate = new ComposedRecordSerializer<>(
                topic,
                keySer,
                valueSer,
                e -> e.valueEnvelope().header().rawMessage().brokerTimestamp(),
                List.of(
                        HeaderPopulators.w3cTrace((SFunction<ErrorEnvelope<CepState, Msg>, com.hcltech.rmg.messages.RawMessage>)
                                e -> e.valueEnvelope().header().rawMessage()),
                        HeaderPopulators.domain(
                                (SFunction<ErrorEnvelope<CepState, Msg>, String>) e -> e.valueEnvelope().header().rawMessage().domainId(),
                                (LFunction<ErrorEnvelope<CepState, Msg>>) e -> e.valueEnvelope().getFullCepStateSize(),
                                (LFunction<ErrorEnvelope<CepState, Msg>>) e -> e.valueEnvelope().getDurationNanos()
                        ),
                        HeaderPopulators.header("stage",
                                (SFunction<ErrorEnvelope<CepState, Msg>, String>) ErrorEnvelope::stageName),
                        HeaderPopulators.header("rawMessage",
                                (SFunction<ErrorEnvelope<CepState, Msg>, String>) e -> e.valueEnvelope().header().rawMessage().rawValue())
                )
        );
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(ErrorEnvelope<CepState, Msg> e, KafkaSinkContext ctx, Long ts) {
        return delegate.serialize(e, ctx, ts);
    }
}
