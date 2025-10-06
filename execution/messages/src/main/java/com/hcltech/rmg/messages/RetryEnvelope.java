package com.hcltech.rmg.messages;

public record RetryEnvelope< T>(ValueEnvelope< T> envelope,
                                         String stageName,
                                         int retryCount
) implements Envelope< T> {
    @Override
    public ValueEnvelope< T> valueEnvelope() {
        return envelope;
    }
}
