package com.hcltech.rmg.flinkadapters.envelopes;

import com.hcltech.rmg.common.Codec;

import java.util.Map;

public interface Envelopes {
    public static <T> Codec<ValueEnvelope<T>, String> valueEnvelopeStringCodec(Codec<T, Map<String, Object>> dataCodec) {
        return new ValueEnvelopeStringCodec<T>(dataCodec);
    }

    public static <T> Codec<RetryEnvelope<T>, String> retryEnvelopeStringCodec(Codec<T, Map<String, Object>> dataCodec) {
        return new RetryEnvelopeStringCodec<>(dataCodec);
    }
}

record ValueEnvelopeStringCodec<T>(Codec<ValueEnvelope<Map<String, Object>>, String> valueCodec,
                                   Codec<T, Map<String, Object>> dataCodec) implements Codec<ValueEnvelope<T>, String> {

    ValueEnvelopeStringCodec(Codec<T, Map<String, Object>> dataCodec) {
        this((Codec) Codec.clazzCodec(ValueEnvelope.class), dataCodec);
    }

    @Override
    public String encode(ValueEnvelope<T> value) throws Exception {
        Map<String, Object> dataAsMaps = dataCodec.encode(value.data());
        ValueEnvelope<Map<String, Object>> newEnv = value.withData(dataAsMaps);
        return valueCodec.encode(newEnv);
    }


    @Override
    public ValueEnvelope<T> decode(String encoded) throws Exception {
        var envWithMaps = valueCodec.decode(encoded);
        var data = dataCodec.decode(envWithMaps.data());
        return envWithMaps.withData(data);
    }
}

record RetryEnvelopeStringCodec<T>(Codec<RetryEnvelope<Map<String, Object>>, String> retryCodec,
                                   Codec<T, Map<String, Object>> dataCodec) implements Codec<RetryEnvelope<T>, String> {

    RetryEnvelopeStringCodec(Codec<T, Map<String, Object>> dataCodec) {
        this((Codec) Codec.clazzCodec(RetryEnvelope.class), dataCodec);
    }

    @Override
    public String encode(RetryEnvelope<T> value) throws Exception {
        Map<String, Object> dataAsMaps = dataCodec.encode(value.envelope().data());
        RetryEnvelope<Map<String, Object>> newEnv = value.withData(dataAsMaps);
        return retryCodec.encode(newEnv);
    }

    @Override
    public RetryEnvelope<T> decode(String encoded) throws Exception {
        var envWithMaps = retryCodec.decode(encoded);
        var data = dataCodec.decode(envWithMaps.envelope().data());
        return envWithMaps.withData(data);
    }
}
