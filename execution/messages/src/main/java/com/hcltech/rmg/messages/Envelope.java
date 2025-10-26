package com.hcltech.rmg.messages;

import com.hcltech.rmg.config.configs.Configs;

import java.util.function.Function;

public interface Envelope<CepState, Msg> {
    ValueEnvelope<CepState, Msg> valueEnvelope();

    default String domainId() {
        return valueEnvelope().header().rawMessage().domainId();
    }

    default EnvelopeHeader<CepState> header() {
        return valueEnvelope().header();
    }

    default String keyForModule(String module) {
        var ve = valueEnvelope();
        var paramKey = ve.header().parameters().key();
        var event = ve.header().eventType();
        var fullKey = Configs.composeKey(paramKey, event, module);
        return fullKey;
    }

    default Envelope<CepState, Msg> map(Function<ValueEnvelope<CepState, Msg>, Envelope<CepState, Msg>> mapper) {
        return this;
    }


}
