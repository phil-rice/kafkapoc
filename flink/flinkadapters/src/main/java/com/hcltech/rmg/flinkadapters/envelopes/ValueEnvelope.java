package com.hcltech.rmg.flinkadapters.envelopes;

import com.hcltech.rmg.flinkadapters.kafka.RawKafkaData;

public record ValueEnvelope<T>(String domainType, String domainId, T data, int lane,
                               RawKafkaData rawKafkaData) implements ValueRetryErrorEnvelope, ValueRetryEnvelope, ValueErrorEnvelope<T> {
    public <T1> ValueEnvelope<T1> mapData(java.util.function.Function<T, T1> mapper) {
        return withData(mapper.apply(data));
    }

    public PartLane partitionLane() {
        return new PartLane(rawKafkaData.partition(), lane);
    }
    public <T1> ValueEnvelope<T1> withData(T1 newData) {
        return new ValueEnvelope<>(domainType, domainId, newData, lane, rawKafkaData);
    }


    public RetryEnvelope<T> toRetryEnvelope(String stageName, int count) {
        return new RetryEnvelope<>(this, stageName, count);
    }

    public ErrorEnvelope<T> toErrorEnvelope(String stageName, java.util.List<String> errorMessages) {
        return new ErrorEnvelope<>(this, stageName, errorMessages);
    }
}
