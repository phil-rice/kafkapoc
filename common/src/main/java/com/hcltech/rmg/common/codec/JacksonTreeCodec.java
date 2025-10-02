package com.hcltech.rmg.common.codec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.Map;

public class JacksonTreeCodec<T> implements Codec<T, Map<String, Object>>, HasObjectMapper {
    private final Class<T> klass;
    private final ObjectMapper mapper;

    public JacksonTreeCodec(ObjectMapper mapper, Class<T> klass) {
        // copy so we don't mutate a shared mapper reference
        this.mapper = mapper.copy()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.klass = klass;
    }

    public JacksonTreeCodec(Class<T> klass) {
        this(new ObjectMapper(), klass);
    }

    @Override
    public ErrorsOr<Map<String, Object>> encode(T from) {
        try {
            return ErrorsOr.lift(mapper.convertValue(from, new TypeReference<Map<String, Object>>() {
            }));
        } catch (Exception e) {
            return ErrorsOr.error("Failed to encode to Map: " + e.getMessage());
        }

    }

    @Override
    public ErrorsOr<T> decode(Map<String, Object> map) {
        try {
            return ErrorsOr.lift(mapper.convertValue(map, klass));
        } catch (Exception e) {
            return ErrorsOr.error("Failed to decode from Map: " + e.getMessage());
        }
    }

    @Override
    public ObjectMapper objectMapper() {
        return mapper;
    }

}
