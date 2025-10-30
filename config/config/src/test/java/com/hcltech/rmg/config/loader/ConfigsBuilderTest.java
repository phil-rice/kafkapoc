package com.hcltech.rmg.config.loader;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.configs.Configs;
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

    @Test
    void propagates_root_fields_into_each_config() throws Exception {
        // given a root with 2×2 permutations (same one used in happyPath)
        RootConfig root = loadRoot("root.json");

        ErrorsOr<Configs> eo = ConfigsBuilder.buildFromClasspath(
                root,
                dashedKey(),                         // dev-uk, dev-de, prod-uk, prod-de
                under("ConfigsBuilderTest/behaviors", ".json"),
                cl()
        );
        assertTrue(eo.isValue(), () -> "Expected value but got: " + eo.getErrors());
        Configs cfgs = eo.valueOrThrow();

        // pick one config and verify RootConfig fields were propagated
        var one = cfgs.getConfig("dev-uk").valueOrThrow();
        assertSame(root.parameterConfig(), one.parameterConfig(),
                "Configs should reuse the RootConfig's parameterConfig reference");
        assertEquals(root.xmlSchemaPath(), one.xmlSchemaPath(),
                "Configs should copy the RootConfig's xmlSchemaPath");
    }

    @Test
    void builds_single_permutation_and_loads_exact_behavior() throws Exception {
        // Tight root: only one permutation (env=dev, country=uk)
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
        var rc = RootConfigLoader.fromJson(rootJson).valueOrThrow();

        ErrorsOr<Configs> eo = ConfigsBuilder.buildFromClasspath(
                rc,
                dashedKey(), // -> "dev-uk"
                under("ConfigsBuilderTest/behaviors", ".json"),
                cl()
        );
        assertTrue(eo.isValue(), () -> "Expected value but got: " + eo.getErrors());

        Configs cfgs = eo.valueOrThrow();
        assertEquals(1, cfgs.keyToConfigMap().size(), "Exactly one permutation expected");

        var only = cfgs.getConfig("dev-uk").valueOrThrow().behaviorConfig().events();
        assertTrue(only.containsKey("evt-dev-uk"), "Must load the exact behavior for the single key");
    }
    @Test
    void safeApply_keyFn_returns_blank_is_reported() throws Exception {
        RootConfig root = loadRoot("root.json"); // any valid root works

        Function<List<String>, String> blankKey = vals -> "   "; // triggers keyFn blank
        Function<List<String>, String> anyRes = under("ConfigsBuilderTest/behaviors", ".json");

        var eo = ConfigsBuilder.buildFromClasspath(root, blankKey, anyRes, cl());

        assertTrue(eo.isError());
        assertTrue(
                eo.errorsOrThrow().stream().anyMatch(m -> m.startsWith("keyFn returned blank for values=")),
                "Should report keyFn returned blank"
        );
    }

    @Test
    void safeApply_keyFn_throws_is_reported() throws Exception {
        RootConfig root = loadRoot("root.json");

        Function<List<String>, String> throwingKey = vals -> { throw new RuntimeException("boom-key"); };
        Function<List<String>, String> anyRes = under("ConfigsBuilderTest/behaviors", ".json");

        var eo = ConfigsBuilder.buildFromClasspath(root, throwingKey, anyRes, cl());

        assertTrue(eo.isError());
        assertTrue(
                eo.errorsOrThrow().stream().anyMatch(m -> m.contains("keyFn threw for values=") && m.contains("boom-key")),
                "Should report keyFn threw with message"
        );
    }

    @Test
    void safeApply_resourceFn_returns_blank_is_reported() throws Exception {
        RootConfig root = loadRoot("root.json");

        Function<List<String>, String> okKey = ConfigsBuilderTest::joinDash;
        Function<List<String>, String> blankRes = vals -> "  "; // triggers resourceFn blank

        var eo = ConfigsBuilder.buildFromClasspath(root, okKey, blankRes, cl());

        assertTrue(eo.isError());
        assertTrue(
                eo.errorsOrThrow().stream().anyMatch(m -> m.startsWith("resourceFn returned blank for values=")),
                "Should report resourceFn returned blank"
        );
    }

    @Test
    void safeApply_resourceFn_throws_is_reported() throws Exception {
        RootConfig root = loadRoot("root.json");

        Function<List<String>, String> okKey = ConfigsBuilderTest::joinDash;
        Function<List<String>, String> throwingRes = vals -> { throw new RuntimeException("boom-res"); };

        var eo = ConfigsBuilder.buildFromClasspath(root, okKey, throwingRes, cl());

        assertTrue(eo.isError());
        assertTrue(
                eo.errorsOrThrow().stream().anyMatch(m -> m.contains("resourceFn threw for values=") && m.contains("boom-res")),
                "Should report resourceFn threw with message"
        );
    }

    @Test
    void failed_to_load_behavior_when_json_is_bad_is_reported() {
        // minimal root with a single permutation so we only hit this once
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
        RootConfig root = RootConfigLoader.fromJson(rootJson).valueOrThrow();

        // Key: dev-uk; Resource: "bad/behavior.json"
        Function<List<String>, String> keyFn = ConfigsBuilderTest::joinDash;
        Function<List<String>, String> resourceFn = vals -> "bad/behavior.json";

        // ClassLoader that returns malformed JSON for that resource
        ClassLoader badJsonCL = new ClassLoader(cl()) {
            @Override public InputStream getResourceAsStream(String name) {
                if ("bad/behavior.json".equals(name)) {
                    return new java.io.ByteArrayInputStream("{ not valid JSON ".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                }
                return super.getResourceAsStream(name);
            }
        };

        var eo = ConfigsBuilder.buildFromClasspath(root, keyFn, resourceFn, badJsonCL);

        assertTrue(eo.isError(), "Expected an error for malformed behavior JSON");
        assertTrue(
                eo.errorsOrThrow().stream().anyMatch(m ->
                        m.startsWith("Failed to load behavior 'bad/behavior.json' (values=[dev, uk]):")
                ),
                "Should prefix with 'Failed to load behavior' and include resource + values"
        );
    }

    /* ---- small helper to avoid lambda duplication ---- */
    private static String joinDash(java.util.List<String> values) {
        return String.join("-", values);
    }

}
