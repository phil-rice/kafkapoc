package com.hcltech.rmg.messages;

public interface Envelope<CepState, Msg> {
    ValueEnvelope<CepState, Msg> valueEnvelope();

    default EnvelopeHeader<CepState> header() {
        return valueEnvelope().header();
    }


}
