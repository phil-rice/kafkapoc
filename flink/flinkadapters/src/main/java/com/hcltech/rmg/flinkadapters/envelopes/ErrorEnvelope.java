package com.hcltech.rmg.flinkadapters.envelopes;

import java.util.List;

public record ErrorEnvelope<T>(ValueEnvelope<T> envelope,
                               String stageName,
                               List<String> errorMessages
) implements ValueRetryErrorEnvelope {
    @Override
    public String domainId() {
        return envelope.domainId();
    }

    public <T1> ErrorEnvelope<T1> mapData(java.util.function.Function<T, T1> mapper) {
        return new ErrorEnvelope<>(envelope.mapData(mapper), stageName, errorMessages);
    }

    public <T1> ErrorEnvelope<T1> withData(T1 newData) {
        return new ErrorEnvelope<>(envelope.withData(newData), stageName, errorMessages);
    }
}
