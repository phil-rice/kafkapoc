package com.hcltech.rmg.common.codec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.Objects;

public final class JacksonTypedJsonCodec<T> implements Codec<T, String>, HasObjectMapper {
    private final ObjectMapper mapper;
    private final Class<T> klass;
    private final TypeReference<T> typeRef; // optional for generic types

    public JacksonTypedJsonCodec(Class<T> klass) {
        this(new ObjectMapper(), klass);
    }

    public JacksonTypedJsonCodec(ObjectMapper baseMapper, Class<T> klass) {
        this.mapper = Objects.requireNonNull(baseMapper).copy();
        this.mapper.findAndRegisterModules();
        this.klass = Objects.requireNonNull(klass);
        this.typeRef = null;
    }

    /**
     * Use this ctor if T is generic (e.g., List<MyType>)
     */
    public JacksonTypedJsonCodec(TypeReference<T> typeRef) {
        this(new ObjectMapper(), typeRef);
    }

    public JacksonTypedJsonCodec(ObjectMapper baseMapper, TypeReference<T> typeRef) {
        this.mapper = Objects.requireNonNull(baseMapper).copy();
        this.mapper.findAndRegisterModules();
        this.klass = null;
        this.typeRef = Objects.requireNonNull(typeRef);
    }

    @Override
    public ErrorsOr<String> encode(T value) {
        try {
            return ErrorsOr.lift(mapper.writeValueAsString(value));
        } catch (Exception e) {
            return ErrorsOr.error("Failed to encode to JSON: " + e.getMessage());
        }
    }

    @Override
    public ErrorsOr<T> decode(String json) {
        try {
            return ErrorsOr.lift(klass != null ? mapper.readValue(json, klass) : mapper.readValue(json, typeRef));
        } catch (Exception e) {
            return ErrorsOr.error("Failed to decode from JSON: " + e.getMessage());
        }
    }

    @Override
    public ObjectMapper objectMapper() {
        return mapper;
    }

}
