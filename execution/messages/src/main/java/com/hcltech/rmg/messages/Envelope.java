package com.hcltech.rmg.messages;

public interface Envelope<CEPState, T> {
    ValueEnvelope<CEPState, T> valueEnvelope();

    default EnvelopeHeader<CEPState> header() {
        return valueEnvelope().header();
    }


}
