package com.hcltech.rmg.messages;

import com.hcltech.rmg.config.configs.Configs;
import com.hcltech.rmg.execution.aspects.AspectExecutor;

import java.util.function.Function;

public interface Envelope<CepState, Msg> {
    ValueEnvelope<CepState, Msg> valueEnvelope();

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

    default <Component> Envelope<CepState, Msg> map(AspectExecutor<Component, ValueEnvelope<CepState, Msg>, Envelope<CepState, Msg>> executor, String key, Component aspect) {
        return this;
    }

}
