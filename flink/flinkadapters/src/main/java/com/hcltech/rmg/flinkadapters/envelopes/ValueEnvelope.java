package com.hcltech.rmg.flinkadapters.envelopes;

public record ValueEnvelope<T>(String domainType, String domainId, T data,
                               long eventTimeMillis) implements ValueRetryErrorEnvelope, ValueRetryEnvelope {
    public <T1> ValueEnvelope<T1> mapData(java.util.function.Function<T, T1> mapper) {
        return withData(mapper.apply(data));
    }

    public <T1> ValueEnvelope<T1> withData(T1 newData) {
        return new ValueEnvelope<>(domainType, domainId, newData,  eventTimeMillis);
    }


    public RetryEnvelope<T> toRetryEnvelope(String stageName,int count) {
        return new RetryEnvelope<>(this,stageName, count);
    }

    public ErrorEnvelope<T> toErrorEnvelope(String stageName,java.util.List<String> errorMessages) {
        return new ErrorEnvelope<>(this,stageName, errorMessages);
    }
}
