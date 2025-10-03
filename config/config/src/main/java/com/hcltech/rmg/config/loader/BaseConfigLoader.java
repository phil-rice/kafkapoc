package com.hcltech.rmg.config.loader;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public interface BaseConfigLoader {
    static ObjectMapper base(ObjectMapper om) {
        return om
                // Open to extension: ignore extra fields in JSON
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

                // Let record constructors enforce required fields & defaults
                .configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, false)
                .configure(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES, false)

                // No silent coercion of single value -> array
                .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, false)

                // Keep property names case-sensitive
                .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, false)

                // Nice for human-authored JSON files
                .enable(JsonReadFeature.ALLOW_JAVA_COMMENTS.mappedFeature())
                .enable(JsonReadFeature.ALLOW_TRAILING_COMMA.mappedFeature());
    }

}
