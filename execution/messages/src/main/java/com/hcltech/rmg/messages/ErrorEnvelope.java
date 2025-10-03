package com.hcltech.rmg.messages;

import java.util.List;

public record ErrorEnvelope<CEPState, T>(ValueEnvelope<CEPState, T> envelope,
                                         String stageName,
                                         List<String> errors
) implements Envelope<CEPState, T> {

    @Override
    public ValueEnvelope<CEPState, T> valueEnvelope() {
        return envelope;
    }
}
