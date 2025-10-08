package com.hcltech.rmg.messages;

public record RetryEnvelope< CepState,T>(ValueEnvelope< CepState,T> envelope,
                                         String stageName,
                                         int retryCount
) implements Envelope< CepState,T> {
    @Override
    public ValueEnvelope<CepState, T> valueEnvelope() {
        return envelope;
    }
}
