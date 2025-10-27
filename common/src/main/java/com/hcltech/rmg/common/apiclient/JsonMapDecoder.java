package com.hcltech.rmg.common.apiclient;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.function.Function;

/**
 * Decode function that parses JSON strings into a Map<String, Object>.
 * Nested structures (arrays, objects) become Lists and Maps automatically.
 */
public final class JsonMapDecoder implements Function<String, Map<String, Object>> {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> TYPE = new TypeReference<>() {};

  @Override
  public Map<String, Object> apply(String body) {
    if (body == null || body.isBlank()) {
      return Map.of(); // empty response → empty map
    }
    try {
      return MAPPER.readValue(body, TYPE);
    } catch (Exception e) {
      throw new RuntimeException("Failed to decode JSON body: " + body, e);
    }
  }
}
