package com.hcltech.rmg.messages;

public interface Envelope< T> {
    ValueEnvelope< T> valueEnvelope();

    default EnvelopeHeader header() {
        return valueEnvelope().header();
    }


}
