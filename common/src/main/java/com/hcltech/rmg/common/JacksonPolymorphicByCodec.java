package com.hcltech.rmg.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

@SuppressWarnings("unchecked")
public final class JacksonPolymorphicByCodec<T> implements Codec<T, String>, HasObjectMapper {
    private final Function<? super T, String> typeOf;
    private final Map<String, Codec<? extends T, String>> codecs;
    private final ObjectMapper mapper;

    public JacksonPolymorphicByCodec(Function<? super T, String> typeOf,
                                     Map<String, Codec<? extends T, String>> codecs) {
        this(typeOf, codecs, new ObjectMapper().findAndRegisterModules());
    }

    public JacksonPolymorphicByCodec(Function<? super T, String> typeOf,
                                     Map<String, Codec<? extends T, String>> codecs,
                                     ObjectMapper mapper) {
        this.typeOf = Objects.requireNonNull(typeOf, "typeOf");
        this.codecs = Objects.requireNonNull(codecs, "codecs");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    @Override
    public String encode(T from) throws Exception {
        String disc = Objects.requireNonNull(typeOf.apply(from), "typeOf returned null");
        Codec<? extends T, String> sub = codecs.get(disc);
        if (sub == null) {
            throw new IllegalArgumentException("No codec registered for discriminator: " + disc);
        }
        // Encode the subtype to its own JSON
        String payloadJson = ((Codec<T, String>) sub).encode(from);
        JsonNode payloadNode = mapper.readTree(payloadJson);

        ObjectNode wrapper = mapper.createObjectNode();
        wrapper.put("type", disc);
        wrapper.set("payload", payloadNode);

        return mapper.writeValueAsString(wrapper);
    }

    @Override
    public T decode(String to) throws Exception {
        JsonNode root = mapper.readTree(to);
        JsonNode typeNode = root.get("type");
        if (typeNode == null || !typeNode.isTextual()) {
            throw new IllegalArgumentException("Missing textual 'type' field");
        }
        String disc = typeNode.asText();
        Codec<? extends T, String> sub = codecs.get(disc);
        if (sub == null) {
            throw new IllegalArgumentException("Unknown discriminator: " + disc);
        }
        JsonNode payload = root.get("payload");
        if (payload == null) {
            throw new IllegalArgumentException("Missing 'payload' field");
        }
        String payloadJson = mapper.writeValueAsString(payload);
        return ((Codec<T, String>) sub).decode(payloadJson);
    }
    @Override
    public ObjectMapper objectMapper() {
        return mapper;
    }

}
