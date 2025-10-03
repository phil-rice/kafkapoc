package com.hcltech.rmg.messages;

public record ValueEnvelope<CEPState, T>(EnvelopeHeader<CEPState> header, T data) {
    public <T1> ValueEnvelope<CEPState, T1> mapData(java.util.function.Function<T, T1> mapper) {
        return withData(mapper.apply(data));
    }

    public <T1> ValueEnvelope<CEPState, T1> withData(T1 newData) {
        return new ValueEnvelope<>(header, newData);
    }
}
