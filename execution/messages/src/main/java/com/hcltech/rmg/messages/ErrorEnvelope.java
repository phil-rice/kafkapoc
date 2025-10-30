package com.hcltech.rmg.messages;

import java.util.List;

public record ErrorEnvelope<CepState, Msg>(ValueEnvelope<CepState, Msg> envelope,
                                           String stageName,
                                           List<String> errors
) implements Envelope<CepState, Msg> {

    @Override
    public ValueEnvelope<CepState, Msg> valueEnvelope() {
        return envelope;
    }
}
