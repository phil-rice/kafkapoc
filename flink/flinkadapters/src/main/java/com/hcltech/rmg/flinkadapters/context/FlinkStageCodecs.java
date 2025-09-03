package com.hcltech.rmg.flinkadapters.context;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hcltech.rmg.common.Codec;
import com.hcltech.rmg.common.HasObjectMapper;
import com.hcltech.rmg.common.JacksonTreeCodec;
import com.hcltech.rmg.flinkadapters.envelopes.Envelopes;
import com.hcltech.rmg.flinkadapters.envelopes.RetryEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.ValueEnvelope;

import java.io.Serializable;
import java.util.Map;

/**
 * A bundle of codecs derived from a single payload codec.
 * All codecs share the same ObjectMapper for consistency.
 */
public record FlinkStageCodecs<From>(
        Codec<From, Map<String, Object>> payload,
        Codec<ValueEnvelope<From>, String> valueEnvelope,
        Codec<RetryEnvelope<From>, String> retryEnvelope
) implements Serializable {

    public static <From> FlinkStageCodecs<From> fromPayloadCodec(Codec<From, Map<String, Object>> payloadCodec) {
        return new FlinkStageCodecs<From>(payloadCodec, Envelopes.valueEnvelopeStringCodec(payloadCodec), Envelopes.retryEnvelopeStringCodec(payloadCodec));
    }

}
