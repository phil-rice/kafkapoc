package com.hcltech.rmg.flinkadapters.codec;

import com.hcltech.rmg.common.codec.Codec;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FlinkCodecsFactory {
    // static cache: payloadCodec instance -> FlinkStageCodecs
    private static final Map<Codec<?,?>, FlinkStageCodecs<?>> CACHE = new ConcurrentHashMap<>();

        @SuppressWarnings("unchecked")
    static <From> FlinkStageCodecs<From> create(Codec<From, Map<String, Object>> fromCodec) {
        return (FlinkStageCodecs<From>) CACHE.computeIfAbsent(fromCodec, key -> FlinkStageCodecs.fromPayloadCodec((Codec<From, Map<String, Object>>) key));
    }
}
