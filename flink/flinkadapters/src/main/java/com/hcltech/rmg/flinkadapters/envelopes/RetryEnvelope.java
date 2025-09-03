package com.hcltech.rmg.flinkadapters.envelopes;

public record RetryEnvelope<T>(ValueEnvelope<T> envelope,
                               String nodeName, //The node name that we will retry from
                               int retryCount
) {
    public <T1> RetryEnvelope<T1> mapData(java.util.function.Function<T, T1> mapper) {
        return new RetryEnvelope<>(envelope.mapData(mapper), nodeName, retryCount);
    }
    public <T1> RetryEnvelope<T1> withData(T1 newData) {
        return new RetryEnvelope<>(envelope.withData(newData), nodeName, retryCount);
    }
}
