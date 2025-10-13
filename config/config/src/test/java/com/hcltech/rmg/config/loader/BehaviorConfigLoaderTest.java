package com.hcltech.rmg.config.loader;

import com.hcltech.rmg.config.aspect.AspectMap;
import com.hcltech.rmg.config.bizlogic.CelFileLogic;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.enrich.ApiEnrichment;
import com.hcltech.rmg.config.enrich.FixedEnrichment;
import com.hcltech.rmg.config.transformation.XmlTransform;
import com.hcltech.rmg.config.transformation.XsltTransform;
import com.hcltech.rmg.config.validation.CelValidation;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static com.hcltech.rmg.config.fixture.ConfigTestFixture.*;
import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.*;

class BehaviorConfigLoaderTest {

    @Test
    void loads_minimal_config() throws IOException {
        try (var in = resourceStream("config/good-minimal.json")) {
            var actual = BehaviorConfigLoader.validated(BehaviorConfigLoader.fromJson(in));
            var expected = BehaviorConfig.empty();
            assertEquals(expected, actual);
        }
    }

    @Test
    void loads_bizlogic_inline_cel() throws IOException {
        try (var in = resourceStream("config/good-bizlogic-inline.json")) {
            var actual = BehaviorConfigLoader.validated(BehaviorConfigLoader.fromJson(in));
            var expected = new BehaviorConfig(Map.ofEntries(
                    entry("E",
                            new AspectMap(
                                    v(),
                                    t(),
                                    e(),
                                    b(kv("notification", new CelInlineLogic("true?[]:null")))
                            ))
            ));
            assertEquals(expected, actual);
        }
    }

    @Test
    void loads_complex_config() throws IOException {
        try (var in = resourceStream("config/good-complex.json")) {
            var actual = BehaviorConfigLoader.validated(BehaviorConfigLoader.fromJson(in));
            var expected = new BehaviorConfig(Map.ofEntries(
                    entry("readyForDelivery",
                            new AspectMap(
                                    v(kv("notification", new CelValidation("a + b > 0"))),
                                    t(kv("notification", new XmlTransform("schemas/ready.xsd"))),
                                    e(
                                            kv("api", new ApiEnrichment("http://example", Map.of("q", "1"))),
                                            kv("fixed", new FixedEnrichment(
                                                    // inputs
                                                    List.of(List.of("addr","line1"), List.of("addr","line2")),
                                                    // output
                                                    List.of("addr","postcode"),
                                                    // lookup (values not used by loader logic, but must round-trip)
                                                    Map.of("L1.L2", "PC1")
                                            ))
                                    ),
                                    b(
                                            kv("fileLogic", new CelFileLogic("logic.cel")),
                                            kv("inlineLogic", new CelInlineLogic("a + b"))
                                    )
                            ))
            ));
            assertEquals(expected, actual);
        }
    }


    @Test
    void fails_on_bad_missing_leaf() {
        assertThrows(Exception.class, () -> {
            try (var in = resourceStream("config/bad-missing-leaf.json")) {
                // CelFileLogic ctor should fail while parsing
                BehaviorConfigLoader.fromJson(in);
            }
        });
    }

    @Test
    void succeeds_on_good_unknown_leaf_field_as_given() throws IOException {
        try (var in = resourceStream("config/good-unknown-leaf-field.json")) {
            var actual = BehaviorConfigLoader.validated(BehaviorConfigLoader.fromJson(in));
            var expected = new BehaviorConfig(Map.ofEntries(
                    entry("readyForDelivery",
                            new AspectMap(
                                    v(kv("notification", new CelValidation("somecel"))),
                                    t(),
                                    e(),
                                    b()
                            ))
            ));
            assertEquals(expected, actual);
        }
    }


    @Test
    void fromJson_string_roundtrip_minimal() throws IOException {
        String json = """
            { "events": {} }
        """;
        BehaviorConfig cfg = BehaviorConfigLoader.fromJson(json);
        assertEquals(BehaviorConfig.empty(), BehaviorConfigLoader.validated(cfg));
    }

    @Test
    void fromJson_path_roundtrip_minimal() throws IOException {
        String json = """
            { "events": {} }
        """;
        Path tmp = Files.createTempFile("behavior-config-", ".json");
        Files.writeString(tmp, json);
        try {
            BehaviorConfig cfg = BehaviorConfigLoader.fromJson(tmp);
            assertEquals(BehaviorConfig.empty(), BehaviorConfigLoader.validated(cfg));
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    @Test
    void validated_null_returns_empty() {
        BehaviorConfig out = BehaviorConfigLoader.validated(null);
        assertEquals(BehaviorConfig.empty(), out);
    }



    @Test
    void validated_throws_when_event_has_null_AspectMap() throws IOException {
        String json = """
            { "events": { "E": null } }
        """;
        BehaviorConfig cfg = BehaviorConfigLoader.fromJson(json);

        var ex = assertThrows(IllegalArgumentException.class,
                () -> BehaviorConfigLoader.validated(cfg));
        assertTrue(ex.getMessage().contains("Event 'E' has null AspectMap"), ex.getMessage());
    }



    @Test
    void strictJson_returns_cached_mapper_instance() {
        // Sanity check for the static JSON instance
        assertSame(BehaviorConfigLoader.JSON, BehaviorConfigLoader.strictJson());
    }

    @Test
    void validated_coerces_events_null_to_empty_config_today() throws IOException {
        // Today: BehaviorConfig ctor coerces events == null -> Map.of()
        String json = """
        { "events": null }
    """;
        BehaviorConfig cfg = BehaviorConfigLoader.fromJson(json);

        // This should NOT throw today; it should return an empty config.
        // If a future change stops coercing to empty, this test will fail,
        // and your defensive check ('events' must be present) will then fire instead.
        BehaviorConfig out = BehaviorConfigLoader.validated(cfg);
        assertEquals(BehaviorConfig.empty(), out, "events:null is currently coerced to empty");
    }

    @Test
    void validated_sees_submaps_as_empty_when_null_in_json_today() throws IOException {
        // Today: AspectMap ctor coerces each submap from null -> Map.of()
        String json = """
        {
          "events": {
            "E": {
              "validation": null,
              "transformation": null,
              "enrichment": null,
              "bizlogic": null
            }
          }
        }
    """;
        BehaviorConfig cfg = BehaviorConfigLoader.fromJson(json);

        // This should NOT throw today; it should see all non-null empty maps.
        // If a future change stops coercing, this test will fail first — then
        // your defensive IllegalStateException in validated(...) would become reachable.
        BehaviorConfig out = BehaviorConfigLoader.validated(cfg);

        var expected = new BehaviorConfig(
                java.util.Map.of("E", new com.hcltech.rmg.config.aspect.AspectMap(
                        java.util.Map.of(),
                        java.util.Map.of(),
                        java.util.Map.of(),
                        java.util.Map.of()
                ))
        );
        assertEquals(expected, out, "null submaps are currently coerced to empty maps");
    }

}
