package com.hcltech.rmg.flinkadapters.envelopes;

public record RetryEnvelope<T>(ValueEnvelope<T> envelope,
                               String stageName,
                               int retryCount
) implements ValueRetryErrorEnvelope, ValueRetryEnvelope {
    @Override
    public String domainId() {
        return envelope.domainId();
    }

    public <T1> RetryEnvelope<T1> mapData(java.util.function.Function<T, T1> mapper) {
        return new RetryEnvelope<>(envelope.mapData(mapper), stageName, retryCount);
    }

    public <T1> RetryEnvelope<T1> withData(T1 newData) {
        return new RetryEnvelope<>(envelope.withData(newData), stageName, retryCount);
    }

    public RetryEnvelope<T> failedRetryEnvelope() {
        return new RetryEnvelope<>(envelope, stageName, retryCount + 1);
    }
}
