package com.hcltech.rmg.messages;

public record AiFailureEnvelope<CepState, Msg>(
        ValueEnvelope<CepState, Msg> valueEnvelope) implements Envelope<CepState, Msg> {
}
