package com.hcltech.rmg.interfaces.envelope;

import java.util.List;

public record ErrorEnvelopeNew<CEPState, T>(ValueEnvelopeNew<CEPState, T> envelope,
                                            String stageName,
                                            List<String> errors
) implements Envelope<CEPState, T> {

    @Override
    public ValueEnvelopeNew<CEPState, T> valueEnvelope() {
        return envelope;
    }
}
