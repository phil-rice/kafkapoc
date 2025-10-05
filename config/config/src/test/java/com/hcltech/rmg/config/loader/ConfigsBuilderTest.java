// src/test/java/com/hcltech/rmg/config/loader/ConfigsBuilderTest.java
package com.hcltech.rmg.config.loader;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.config.Configs;
import com.hcltech.rmg.config.config.RootConfig;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class ConfigsBuilderTest {

    private ClassLoader cl() {
        return getClass().getClassLoader();
    }

    private String resourceString(String name) throws Exception {
        String path = "ConfigsBuilderTest/" + name;
        try (InputStream in = cl().getResourceAsStream(path)) {
            assertNotNull(in, "Missing test resource: " + path);
            return new String(in.readAllBytes());
        }
    }

    private RootConfig loadRoot(String name) throws Exception {
        String json = resourceString(name);
        ErrorsOr<RootConfig> eo = RootConfigLoader.fromJson(json);
        assertTrue(eo.isValue(), () -> "Root load failed: " + eo.getErrors());
        return eo.valueOrThrow();
    }

    private static Function<List<String>, String> dashedKey() {
        return values -> String.join("-", values);
    }

    private static Function<List<String>, String> under(String base, String suffix) {
        return values -> base + "/" + String.join("/", values) + suffix;
    }

    @Test
    void happyPath_buildsAllConfigs_andLoadsCorrectBehaviorPerKey() throws Exception {
        RootConfig root = loadRoot("root.json");

        ErrorsOr<Configs> eo = ConfigsBuilder.buildFromClasspath(
                root,
                dashedKey(),                         // dev-uk, dev-de, prod-uk, prod-de
                under("ConfigsBuilderTest/behaviors", ".json"),
                cl()
        );
        assertTrue(eo.isValue(), () -> "Expected value but got: " + eo.getErrors());
        Configs cfgs = eo.valueOrThrow();

        Map<String, ?> map = cfgs.keyToConfigMap();
        assertEquals(4, map.size());
        assertTrue(map.containsKey("dev-uk"));
        assertTrue(map.containsKey("dev-de"));
        assertTrue(map.containsKey("prod-uk"));
        assertTrue(map.containsKey("prod-de"));

        // Assert distinct markers prove the correct resource got loaded for each key
        var devUk  = cfgs.getConfig("dev-uk").valueOrThrow().behaviorConfig().events();
        var devDe  = cfgs.getConfig("dev-de").valueOrThrow().behaviorConfig().events();
        var prodUk = cfgs.getConfig("prod-uk").valueOrThrow().behaviorConfig().events();
        var prodDe = cfgs.getConfig("prod-de").valueOrThrow().behaviorConfig().events();

        assertTrue(devUk.containsKey("evt-dev-uk"));
        assertTrue(devDe.containsKey("evt-dev-de"));
        assertTrue(prodUk.containsKey("evt-prod-uk"));
        assertTrue(prodDe.containsKey("evt-prod-de"));

        // And make sure cross-mismatches DON'T appear
        assertFalse(devUk.containsKey("evt-prod-uk"));
        assertFalse(devUk.containsKey("evt-dev-de"));
        assertFalse(prodDe.containsKey("evt-dev-uk"));
    }

    @Test
    void missingResource_returnsErrors() throws Exception {
        RootConfig root = loadRoot("root.json");

        // Point to a base where at least one file is intentionally missing
        ErrorsOr<Configs> eo = ConfigsBuilder.buildFromClasspath(
                root,
                dashedKey(),
                under("ConfigsBuilderTest/behaviors-missing", ".json"),
                cl()
        );

        assertTrue(eo.isError(), "Expected errors for missing resources");
        assertTrue(eo.getErrors().stream().anyMatch(s -> s.contains("Missing behavior resource on classpath")),
                "Expected a missing resource error, got: " + eo.getErrors());
    }

    @Test
    void keyFn_duplicateKeys_reportsError() throws Exception {
        RootConfig root = loadRoot("root.json");

        // Collapse everything to the same key to trigger duplicate detection
        Function<List<String>, String> badKey = vals -> "SAME";

        List<String> errs = ConfigsBuilder.buildFromClasspath(
                root,
                badKey,
                under("ConfigsBuilderTest/behaviors", ".json"),
                cl()
        ).errorsOrThrow();

        // The exact order depends on iteration order of permutations; assert contents rather than full list equality
        assertTrue(errs.stream().anyMatch(s -> s.startsWith("Duplicate key from keyFn: 'SAME'")),
                "Expected duplicate key errors, got: " + errs);
    }

    @Test
    void behavior_allowsCommentsAndTrailingCommas_andLoadsCorrectMarker() throws Exception {
        // Tight param space → only dev-uk to hit our 'comments' behavior JSON
        String rootJson = """
                {
                  "parameterConfig": {
                    "parameters": [
                      { "legalValue": ["dev"], "defaultValue": "dev", "description": "env" },
                      { "legalValue": ["uk"],  "defaultValue": "uk",  "description": "country" }
                    ]
                  },
                  "xmlSchemaPath": "schemas/config.xsd"
                }
                """;
        ErrorsOr<RootConfig> rcEo = RootConfigLoader.fromJson(rootJson);
        assertTrue(rcEo.isValue(), () -> "Root load failed: " + rcEo.getErrors());
        RootConfig root = rcEo.valueOrThrow();

        ErrorsOr<Configs> eo = ConfigsBuilder.buildFromClasspath(
                root,
                dashedKey(), // dev-uk
                under("ConfigsBuilderTest/behaviors-with-comments", ".json"),
                cl()
        );

        assertTrue(eo.isValue(), () -> "Should parse behavior with comments/trailing commas, got: " + eo.getErrors());
        var events = eo.valueOrThrow().getConfig("dev-uk").valueOrThrow()
                .behaviorConfig().events();
        assertTrue(events.containsKey("evt-dev-uk-comments"));
    }
}
