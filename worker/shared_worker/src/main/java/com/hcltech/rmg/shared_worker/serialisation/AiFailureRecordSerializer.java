package com.hcltech.rmg.shared_worker.serialisation;

import com.hcltech.rmg.common.function.LFunction;
import com.hcltech.rmg.common.function.SFunction;
import com.hcltech.rmg.messages.AiFailureEnvelope;
import com.hcltech.rmg.messages.RawMessage;
import com.hcltech.rmg.messages.RetryEnvelope;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

public final class AiFailureRecordSerializer<CepState, Msg>
        implements KafkaRecordSerializationSchema<AiFailureEnvelope<CepState, Msg>> {

    private final ComposedRecordSerializer<AiFailureEnvelope<CepState, Msg>> delegate;

    public AiFailureRecordSerializer(String topic,
                                     SerializationSchema<AiFailureEnvelope<CepState, Msg>> keySer,
                                     SerializationSchema<AiFailureEnvelope<CepState, Msg>> valueSer) {

        this.delegate = new ComposedRecordSerializer<AiFailureEnvelope<CepState, Msg>>(
                topic,
                keySer,
                valueSer,
                r -> r.valueEnvelope().header().rawMessage().brokerTimestamp(),
                List.of(
                        HeaderPopulators.w3cTrace((SFunction<AiFailureEnvelope<CepState, Msg>, RawMessage>)
                                r -> r.valueEnvelope().header().rawMessage()),
                        HeaderPopulators.domain(
                                (SFunction<AiFailureEnvelope<CepState, Msg>, String>) r -> r.valueEnvelope().header().rawMessage().domainId(),
                                (LFunction<AiFailureEnvelope<CepState, Msg>>) e -> e.valueEnvelope().getFullCepStateSize(),
                                (LFunction<AiFailureEnvelope<CepState, Msg>>) r -> r.valueEnvelope().getDurationNanos()
                        )
                )
        );
    }


    @Override
    public ProducerRecord<byte[], byte[]> serialize(AiFailureEnvelope<CepState, Msg> r, KafkaSinkContext ctx, Long ts) {
        return delegate.serialize(r, ctx, ts);
    }
}
