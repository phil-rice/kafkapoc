package com.hcltech.rmg.flinkadapters.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class RawKafkaDataDeserializer implements KafkaRecordDeserializationSchema<RawKafkaData> {

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> rec, Collector<RawKafkaData> out) {
        String v = rec.value() == null ? null : new String(rec.value(), java.nio.charset.StandardCharsets.UTF_8);
        String k = rec.key() == null ? null : new String(rec.key(), java.nio.charset.StandardCharsets.UTF_8);
        out.collect(new RawKafkaData(v, k, rec.partition(), rec.offset(), rec.timestamp()));
    }

    @Override
    public TypeInformation<RawKafkaData> getProducedType() {
        return TypeInformation.of(RawKafkaData.class);
    }
}
