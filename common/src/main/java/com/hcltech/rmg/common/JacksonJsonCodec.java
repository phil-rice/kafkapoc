package com.hcltech.rmg.common;

import com.fasterxml.jackson.databind.ObjectMapper;

public final class JacksonJsonCodec implements Codec<Object, String> {
    private final ObjectMapper mapper;

    public JacksonJsonCodec() {
        this.mapper = new ObjectMapper();
        this.mapper.findAndRegisterModules();
    }

    @Override
    public String encode(Object value) throws Exception {
        return mapper.writeValueAsString(value);
    }

    @Override
    public Object decode(String json) throws Exception {
        // Decodes to Maps/Lists/Strings/Numbers/Booleans/null
        return mapper.readValue(json, Object.class);
    }
}
