package com.hcltech.rmg.messages;

public record ValueEnvelope<T>(EnvelopeHeader header, T data) implements Envelope<T> {
    public <T1> ValueEnvelope<T1> withData(T1 newData) {
        return new ValueEnvelope<>(header, newData);
    }

    @Override
    public ValueEnvelope<T> valueEnvelope() {
        return this;
    }
}
