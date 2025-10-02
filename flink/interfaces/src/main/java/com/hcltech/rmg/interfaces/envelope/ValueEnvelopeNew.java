package com.hcltech.rmg.interfaces.envelope;

public record ValueEnvelopeNew<CEPState, T>(EnvelopeHeader<CEPState> header, T data) {
    public <T1> ValueEnvelopeNew<CEPState, T1> mapData(java.util.function.Function<T, T1> mapper) {
        return withData(mapper.apply(data));
    }

    public <T1> ValueEnvelopeNew<CEPState, T1> withData(T1 newData) {
        return new ValueEnvelopeNew<>(header, newData);
    }
}
