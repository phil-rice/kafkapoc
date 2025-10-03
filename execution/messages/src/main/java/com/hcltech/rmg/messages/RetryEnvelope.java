package com.hcltech.rmg.messages;

public record RetryEnvelope<CEPState, T>(ValueEnvelope<CEPState, T> envelope,
                                         String stageName,
                                         int retryCount
) implements Envelope<CEPState, T> {
    @Override
    public ValueEnvelope<CEPState, T> valueEnvelope() {
        return envelope;
    }
}
