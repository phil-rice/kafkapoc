package com.hcltech.rmg.messages;

import com.hcltech.rmg.cepstate.CepEvent;

import java.util.List;

public record ValueEnvelope<CepState, Msg>(EnvelopeHeader<CepState> header,
                                           Msg data,
                                           List<CepEvent> cepStateModifications) implements Envelope<CepState, Msg> {
    public ValueEnvelope<CepState,Msg> withData(Msg newData) {
        return new ValueEnvelope<>(header, newData, cepStateModifications);
    }
    @Override
    public ValueEnvelope<CepState, Msg> valueEnvelope() {
        return this;
    }
}
