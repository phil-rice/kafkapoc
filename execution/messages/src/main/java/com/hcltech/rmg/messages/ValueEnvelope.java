package com.hcltech.rmg.messages;

import com.hcltech.rmg.cepstate.CepEvent;

import java.util.List;

public record ValueEnvelope<CepState, Msg>(EnvelopeHeader<CepState> header,
                                           Msg data,
                                           List<CepEvent> cepStateModifications) implements Envelope<CepState, Msg> {
    @Override
    public ValueEnvelope<CepState, Msg> valueEnvelope() {
        return this;
    }
}
