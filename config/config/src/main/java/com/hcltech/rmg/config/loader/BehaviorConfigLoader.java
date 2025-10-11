package com.hcltech.rmg.config.loader;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.hcltech.rmg.config.loader.BaseConfigLoader.base;

public interface BehaviorConfigLoader {
    static ObjectMapper newMapper() {
        ObjectMapper m = new ObjectMapper();
        m.getFactory().configure(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION, true);
        m.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return m;
    }
    /* ------------ Cached Jackson instances (thread-safe) ------------ */
    ObjectMapper JSON = base(new ObjectMapper());
    ObjectReader CONFIG_READER = JSON.readerFor(BehaviorConfig.class);

    /* ---------------- Public API ---------------- */

    static BehaviorConfig fromJson(InputStream in) throws IOException {
        return CONFIG_READER.readValue(in);
    }

    static BehaviorConfig fromJson(Path path) throws IOException {
        try (InputStream in = Files.newInputStream(path)) {
            return fromJson(in);
        }
    }

    static BehaviorConfig fromJson(String json) throws IOException {
        try (InputStream in = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            return fromJson(in);
        }
    }

    /**
     * Optional: lightweight validation hook (maps may be empty; leafs must be well-formed).
     */
    static BehaviorConfig validated(BehaviorConfig cfg) {
        if (cfg == null) return BehaviorConfig.empty();
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

    }
