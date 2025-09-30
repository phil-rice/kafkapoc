package com.hcltech.rmg.config.loader;

import com.hcltech.rmg.config.aspect.AspectMap;
import com.hcltech.rmg.config.bizlogic.CelFileLogic;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.enrich.ApiEnrichment;
import com.hcltech.rmg.config.validation.CelValidation;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static com.hcltech.rmg.config.fixture.ConfigTestFixture.*;
import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConfigLoaderTest {

    @Test
    void loads_minimal_config() throws IOException {
        try (var in = resourceStream("config/good-minimal.json")) {
            var actual = ConfigLoader.validated(ConfigLoader.fromJson(in));
            var expected = Config.empty();
            assertEquals(expected, actual);
        }
    }

    @Test
    void loads_bizlogic_inline_cel() throws IOException {
        try (var in = resourceStream("config/good-bizlogic-inline.json")) {
            var actual = ConfigLoader.validated(ConfigLoader.fromJson(in));
            var expected = new Config(Map.ofEntries(
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
            var actual = ConfigLoader.validated(ConfigLoader.fromJson(in));
            var expected = new Config(Map.ofEntries(
                    entry("readyForDelivery",
                            new AspectMap(
                                    v(kv("notification", new CelValidation("a + b > 0"))),
                                    t(kv("notification", new com.hcltech.rmg.config.transformation.XsltTransform("xforms/ready.xslt", "schemas/ready.xml"))),
                                    e(kv("api", new ApiEnrichment("http://example", Map.of("q", "1")))),
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
                ConfigLoader.fromJson(in);
            }
        });
    }

    @Test
    void succeeds_on_good_unknown_leaf_field_as_given() throws IOException {
        try (var in = resourceStream("config/good-unknown-leaf-field.json")) {
            var actual = ConfigLoader.validated(ConfigLoader.fromJson(in));
            var expected = new Config(Map.ofEntries(
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
}
