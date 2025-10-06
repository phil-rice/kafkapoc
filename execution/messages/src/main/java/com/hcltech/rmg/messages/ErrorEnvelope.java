package com.hcltech.rmg.messages;

import java.util.List;

public record ErrorEnvelope<T>(ValueEnvelope<T> envelope,
                               String stageName,
                               List<String> errors
) implements Envelope<T> {

    @Override
    public ValueEnvelope<T> valueEnvelope() {
        return envelope;
    }
}
