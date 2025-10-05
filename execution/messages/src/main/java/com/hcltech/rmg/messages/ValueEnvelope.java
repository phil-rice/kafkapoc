package com.hcltech.rmg.messages;

public record ValueEnvelope<CEPState, T>(EnvelopeHeader<CEPState> header, T data) implements Envelope<CEPState, T> {
    public <T1> ValueEnvelope<CEPState, T1> withData(T1 newData) {
        return new ValueEnvelope<>(header, newData);
    }

    @Override
    public ValueEnvelope<CEPState, T> valueEnvelope() {
        return this;
    }
}
