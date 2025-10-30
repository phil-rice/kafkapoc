package com.hcltech.rmg.common.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hcltech.rmg.common.errorsor.ErrorsOr;

public final class JacksonJsonCodec implements Codec<Object, String>, HasObjectMapper {
    private final ObjectMapper mapper;

    public JacksonJsonCodec() {
        this.mapper = new ObjectMapper();
        this.mapper.findAndRegisterModules();
    }

    @Override
    public ErrorsOr<String> encode(Object value) {
        try {
            return ErrorsOr.lift(mapper.writeValueAsString(value));
        } catch (Exception e) {
            return ErrorsOr.error("", e);
        }
    }

    @Override
    public ErrorsOr<Object> decode(String json) {
        try {
            return ErrorsOr.lift(mapper.readValue(json, Object.class));
        } catch (Exception e) {
            return ErrorsOr.error("", e);
        }
    }

    @Override
    public ObjectMapper objectMapper() {
        return mapper;
    }
}
