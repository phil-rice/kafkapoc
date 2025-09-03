package com.hcltech.rmg.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

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
    public Map<String, Object> encode(T from) {
        return mapper.convertValue(from, new TypeReference<Map<String, Object>>() {});
    }

    @Override
    public T decode(Map<String, Object> map) {
        return mapper.convertValue(map, klass);
    }
    @Override
    public ObjectMapper objectMapper() {
        return mapper;
    }

}
