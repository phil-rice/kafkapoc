package com.hcltech.rmg.interfaces.envelope;

public record RetryEnvelopeNew<CEPState, T>(ValueEnvelopeNew<CEPState, T> envelope,
                                            String stageName,
                                            int retryCount
) implements Envelope<CEPState, T> {
    @Override
    public ValueEnvelopeNew<CEPState, T> valueEnvelope() {
        return envelope;
    }
}
