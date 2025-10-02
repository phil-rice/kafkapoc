package com.hcltech.rmg.flinkadapters.envelopes;

import com.hcltech.rmg.common.codec.Codec;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.flinkadapters.kafka.RawKafkaData;

import java.util.Map;

public interface Envelopes {
    static <T> Codec<ValueEnvelope<T>, String> valueEnvelopeStringCodec(Codec<T, Map<String, Object>> dataCodec) {
        return new ValueEnvelopeStringCodec<T>(dataCodec);
    }

    static <T> Codec<RetryEnvelope<T>, String> retryEnvelopeStringCodec(Codec<T, Map<String, Object>> dataCodec) {
        return new RetryEnvelopeStringCodec<>(dataCodec);
    }

    static <T> Codec<ErrorEnvelope<T>, String> errorEnvelopeStringCodec(Codec<T, Map<String, Object>> dataCodec) {
        return new ErrorEnvelopeStringCodec<>(dataCodec);
    }
}

record ValueEnvelopeStringCodec<T>(Codec<ValueEnvelope<Map<String, Object>>, String> valueCodec,
                                   Codec<T, Map<String, Object>> dataCodec,
                                   Codec<RawKafkaData, Map<String, Object>> rkdCodec) implements Codec<ValueEnvelope<T>, String> {

    ValueEnvelopeStringCodec(Codec<T, Map<String, Object>> dataCodec) {
        this((Codec) Codec.clazzCodec(ValueEnvelope.class), dataCodec, (Codec) Codec.clazzCodec(RawKafkaData.class));
    }

    @Override
    public ErrorsOr<String> encode(ValueEnvelope<T> value) {
        return dataCodec.encode(value.data()).flatMap(dataAsMaps -> valueCodec.encode(value.withData(dataAsMaps)));
    }


    @Override
    public ErrorsOr<ValueEnvelope<T>> decode(String encoded) {
        return valueCodec.decode(encoded).flatMap(envWithMaps ->
                dataCodec.decode(envWithMaps.data()).map(envWithMaps::withData));
    }
}

record RetryEnvelopeStringCodec<T>(Codec<RetryEnvelope<Map<String, Object>>, String> retryCodec,
                                   Codec<T, Map<String, Object>> dataCodec,
                                   Codec<RawKafkaData, Map<String, Object>> rkdCodec) implements Codec<RetryEnvelope<T>, String> {

    RetryEnvelopeStringCodec(Codec<T, Map<String, Object>> dataCodec) {
        this((Codec) Codec.clazzCodec(RetryEnvelope.class), dataCodec, (Codec) Codec.clazzCodec(RawKafkaData.class));
    }


    @Override
    public ErrorsOr<String> encode(RetryEnvelope<T> value) {
        T data = value.envelope().data();
        return dataCodec.encode(data).flatMap(dataAsMaps ->
                retryCodec.encode(value.withData(dataAsMaps)));
    }

    @Override
    public ErrorsOr<RetryEnvelope<T>> decode(String encoded) {
        return retryCodec.decode(encoded).flatMap(envWithMaps ->
                dataCodec.decode(envWithMaps.envelope().data()).map(envWithMaps::withData));
    }
}


record ErrorEnvelopeStringCodec<T>(Codec<ErrorEnvelope<Map<String, Object>>, String> errorCodec,
                                   Codec<T, Map<String, Object>> dataCodec) implements Codec<ErrorEnvelope<T>, String> {

    ErrorEnvelopeStringCodec(Codec<T, Map<String, Object>> dataCodec) {
        this((Codec) Codec.clazzCodec(ErrorEnvelope.class), dataCodec);
    }

    @Override
    public ErrorsOr<String> encode(ErrorEnvelope<T> value) {
        return dataCodec.encode((T) value.envelope().data()).flatMap(dataAsMaps -> {
            ErrorEnvelope<Map<String, Object>> newEnv = value.withData(dataAsMaps);
            return errorCodec.encode(newEnv);

        });
    }

    @Override
    public ErrorsOr<ErrorEnvelope<T>> decode(String encoded) {
        return errorCodec.decode(encoded).flatMap(envWithMaps ->
                dataCodec.decode((Map<String, Object>) envWithMaps.envelope().data())
                        .map(envWithMaps::withData));
    }
}