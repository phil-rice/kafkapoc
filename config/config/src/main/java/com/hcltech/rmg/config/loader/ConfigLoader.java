package com.hcltech.rmg.config.loader;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.hcltech.rmg.config.config.Config;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public interface ConfigLoader {

    /* ------------ Cached Jackson instances (thread-safe) ------------ */
    ObjectMapper JSON = base(new ObjectMapper());
    ObjectReader CONFIG_READER = JSON.readerFor(Config.class);

    /* ---------------- Public API ---------------- */

    static Config fromJson(InputStream in) throws IOException {
        return CONFIG_READER.readValue(in);
    }

    static Config fromJson(Path path) throws IOException {
        try (InputStream in = Files.newInputStream(path)) {
            return fromJson(in);
        }
    }

    static Config fromJson(String json) throws IOException {
        try (InputStream in = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            return fromJson(in);
        }
    }

    /**
     * Optional: lightweight validation hook (maps may be empty; leafs must be well-formed).
     */
    static Config validated(Config cfg) {
        if (cfg == null) return Config.empty();
        if (cfg.events() == null) {
            throw new IllegalArgumentException("'events' map must be present (can be empty)");
        }
        cfg.events().forEach((event, aspectMap) -> {
            if (aspectMap == null) {
                throw new IllegalArgumentException("Event '" + event + "' has null AspectMap");
            }
            if (aspectMap.validation() == null
                    || aspectMap.transformation() == null
                    || aspectMap.enrichment() == null
                    || aspectMap.bizlogic() == null) {
                throw new IllegalStateException(
                        "Aspect maps must default to empty for event '" + event + "'");
            }
        });
        return cfg;
    }

    /* --------------- Jackson setup (kept internal) --------------- */

    static ObjectMapper strictJson() { // keep for backwards-compat if other code calls it
        return JSON;
    }

    private static ObjectMapper base(ObjectMapper om) {
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
