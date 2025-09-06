package com.hcltech.rmg.common.codec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Objects;

public final class JacksonTypedJsonCodec<T> implements Codec<T, String> , HasObjectMapper{
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

    /** Use this ctor if T is generic (e.g., List<MyType>) */
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
    public String encode(T value) throws Exception {
        return mapper.writeValueAsString(value);
    }

    @Override
    public T decode(String json) throws Exception {
        if (klass != null) {
            return mapper.readValue(json, klass);
        }
        return mapper.readValue(json, typeRef);
    }
    @Override
    public ObjectMapper objectMapper() {
        return mapper;
    }

}
