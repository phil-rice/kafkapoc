package com.hcltech.rmg.flinkadapters.envelopes;

import com.hcltech.rmg.common.Codec;

import java.util.Map;

public record ValueEnvelope<T>(T data, String domainType, long eventTimeMillis) {
    public <T1> ValueEnvelope<T1> mapData(java.util.function.Function<T, T1> mapper) {
        return withData(mapper.apply(data));
    }

    public <T1> ValueEnvelope<T1> withData(T1 newData) {
        return new ValueEnvelope<>(newData, domainType, eventTimeMillis);
    }

}
