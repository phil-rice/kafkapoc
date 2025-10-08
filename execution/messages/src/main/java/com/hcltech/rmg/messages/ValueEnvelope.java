package com.hcltech.rmg.messages;

public record ValueEnvelope<CepState, Msg>(EnvelopeHeader<CepState> header, Msg data) implements Envelope<CepState, Msg> {
    public <T1> ValueEnvelope<CepState, T1> withData(T1 newData) {
        return new ValueEnvelope<>(header, newData);
    }

    @Override
    public ValueEnvelope<CepState, Msg> valueEnvelope() {
        return this;
    }
}
