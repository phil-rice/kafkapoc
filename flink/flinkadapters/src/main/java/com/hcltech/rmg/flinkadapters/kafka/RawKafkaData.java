package com.hcltech.rmg.flinkadapters.kafka;

import com.hcltech.rmg.common.codec.Codec;
import com.hcltech.rmg.flinkadapters.envelopes.ValueEnvelope;

import java.util.function.Function;

public record RawKafkaData(String value, String key, int partition, long offset, long timestamp) {
    public <T> ValueEnvelope<T> toValueEnvelope(Codec<T, String> payloadCodec, String domainType, int lane, Function<T, String> domainIdExtractor) {
        try {
            T data = value == null ? null : payloadCodec.decode(value);
            return new ValueEnvelope<>(domainType, domainIdExtractor.apply(data), null, data, lane, this);
        } catch (Exception e) {
            //We will need to work out how to deal with this...
            throw new RuntimeException("Failed to decode Kafka message value", e);
        }
    }
}


