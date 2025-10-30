package com.hcltech.rmg.shared_worker.serialisation;

import com.hcltech.rmg.common.function.LFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

import java.util.List;

public class ComposedRecordSerializer<E> implements KafkaRecordSerializationSchema<E> {
    private final String topic;
    private final SerializationSchema<E> keySer;
    private final SerializationSchema<E> valueSer;
    private final LFunction<E> timestampFn;                  // serializable, primitive
    private final List<HeaderPopulator<E>> headerPopulators;   // serializable

    public ComposedRecordSerializer(
            String topic,
            SerializationSchema<E> keySer,
            SerializationSchema<E> valueSer,
            LFunction<E> timestampFn,
            List<HeaderPopulator<E>> headerPopulators) {
        this.topic = topic;
        this.keySer = keySer;
        this.valueSer = valueSer;
        this.timestampFn = timestampFn;
        this.headerPopulators = headerPopulators;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(E e, KafkaSinkContext ctx, Long flinkTs) {
        byte[] key = keySer == null ? null : keySer.serialize(e);
        byte[] value = valueSer == null ? null : valueSer.serialize(e);

        Long ts = flinkTs;
        if (ts == null && timestampFn != null) {
            long t = timestampFn.apply(e);
            ts = (t >= 0) ? Long.valueOf(t) : null; // single boxing when present
        }

        ProducerRecord<byte[], byte[]> rec = new ProducerRecord<>(topic, null, ts, key, value);
        Headers headers = rec.headers();
        if (headerPopulators != null) {
            for (HeaderPopulator<E> hp : headerPopulators) hp.accept(e, headers);
        }
        return rec;
    }
}
