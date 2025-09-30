package com.hcltech.rmg.config.config;

import com.hcltech.rmg.config.aspect.AspectMap;
import com.hcltech.rmg.config.bizlogic.CelFileLogic;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.enrich.ApiEnrichment;
import com.hcltech.rmg.config.transformation.XsltTransform;
import com.hcltech.rmg.config.validation.CelValidation;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConfigWalkerTest {

    /**
     * Records the order and type of callbacks for verification.
     */
    static class RecordingVisitor implements ConfigVisitor {
        final List<String> calls = new ArrayList<>();

        private void hit(String m) { calls.add(m); }

        @Override public void onConfig(Config config) {
            hit("onConfig");
        }

        @Override public void onEvent(String eventName, AspectMap aspects) {
            hit("onEvent(" + eventName + ")");
        }

        @Override public void onValidation(String e, String n, com.hcltech.rmg.config.validation.ValidationAspect v) {
            hit("onValidation(" + e + "," + n + ")");
        }

        @Override public void onCelValidation(String eventName, String name, CelValidation v) {
            // IMPORTANT: record the specific hook distinctly
            hit("onCelValidation(" + eventName + "," + name + ")");
        }

        @Override public void onTransformation(String e, String n, com.hcltech.rmg.config.transformation.TransformationAspect t) {
            hit("onTransformation(" + e + "," + n + ")");
        }

        @Override public void onXsltTransform(String e, String n, XsltTransform t) {
            hit("onXsltTransform(" + e + "," + n + ")");
        }

        @Override public void onEnrichment(String e, String n, com.hcltech.rmg.config.enrich.EnrichmentAspect a) {
            hit("onEnrichment(" + e + "," + n + ")");
        }

        @Override public void onApiEnrichment(String e, String n, ApiEnrichment a) {
            hit("onApiEnrichment(" + e + "," + n + ")");
        }

        @Override public void onBizLogic(String e, String n, com.hcltech.rmg.config.bizlogic.BizLogicAspect b) {
            hit("onBizLogic(" + e + "," + n + ")");
        }

        @Override public void onCelFileLogic(String e, String n, CelFileLogic b) {
            hit("onCelFileLogic(" + e + "," + n + ")");
        }

        @Override public void onCelInlineLogic(String e, String n, CelInlineLogic b) {
            hit("onCelInlineLogic(" + e + "," + n + ")");
        }
    }

    private static List<String> sorted(List<String> list) {
        return list.stream().sorted().toList();
    }

    @Test
    void walks_full_graph_and_invokes_generic_then_specific() {
        var evt = new AspectMap(
                Map.of(
                        "cel", new CelValidation("a + b > 0")
                ),
                Map.of(
                        "xslt", new XsltTransform("transform.xslt", "transform.xsd")
                ),
                Map.of(
                        "api", new ApiEnrichment("http://example", Map.of("q", "1"))
                ),
                Map.of(
                        "fileLogic", new CelFileLogic("logic.cel"),
                        "inlineLogic", new CelInlineLogic("a + b")
                )
        );
        var config = new Config(Map.of("orderPlaced", evt));

        var v = new RecordingVisitor();
        ConfigWalker.walk(config, v);

        // Expect both generic and specific hooks for each family that has a specific subtype
        var expected = List.of(
                "onConfig",
                "onEvent(orderPlaced)",

                // Validation
                "onValidation(orderPlaced,cel)",
                "onCelValidation(orderPlaced,cel)",

                // Transformation
                "onTransformation(orderPlaced,xslt)",
                "onXsltTransform(orderPlaced,xslt)",

                // Enrichment
                "onEnrichment(orderPlaced,api)",
                "onApiEnrichment(orderPlaced,api)",

                // BizLogic
                "onBizLogic(orderPlaced,fileLogic)",
                "onCelFileLogic(orderPlaced,fileLogic)",
                "onBizLogic(orderPlaced,inlineLogic)",
                "onCelInlineLogic(orderPlaced,inlineLogic)"
        );

        assertEquals(sorted(expected), sorted(v.calls));
    }

    @Test
    void tolerates_empty_and_nulls() {
        var config = new Config(null);
        var v = new RecordingVisitor();
        ConfigWalker.walk(config, v);
        assertEquals(List.of("onConfig"), sorted(v.calls));

        var config2 = new Config(Map.of("emptyEvent", AspectMap.empty()));
        var v2 = new RecordingVisitor();
        ConfigWalker.walk(config2, v2);
        assertEquals(List.of("onConfig", "onEvent(emptyEvent)"), sorted(v2.calls));
    }

    @Test
    void families_produce_expected_hooks() {
        var evt = new AspectMap(
                // include two validations so we can verify both are visited
                Map.of(
                        "v1", new CelValidation("1.json"),
                        "v2", new CelValidation("2.json")
                ),
                Map.of(
                        "t2", new XsltTransform("b.xslt", "b.xsd"),
                        "t1", new XsltTransform("a.xslt", "a.xsd")
                ),
                Map.of(
                        "e2", new ApiEnrichment("u2", Map.of()),
                        "e1", new ApiEnrichment("u1", Map.of())
                ),
                Map.of(
                        "b2", new CelInlineLogic("x"),
                        "b1", new CelFileLogic("f1")
                )
        );
        var config = new Config(Map.of("evt", evt));
        var v = new RecordingVisitor();
        ConfigWalker.walk(config, v);

        // Presence checks (order not important)
        assertTrue(v.calls.contains("onValidation(evt,v1)"));
        assertTrue(v.calls.contains("onValidation(evt,v2)"));
        assertTrue(v.calls.contains("onCelValidation(evt,v1)"));
        assertTrue(v.calls.contains("onCelValidation(evt,v2)"));

        assertTrue(v.calls.contains("onTransformation(evt,t1)"));
        assertTrue(v.calls.contains("onTransformation(evt,t2)"));
        assertTrue(v.calls.contains("onXsltTransform(evt,t1)"));
        assertTrue(v.calls.contains("onXsltTransform(evt,t2)"));

        assertTrue(v.calls.contains("onEnrichment(evt,e1)"));
        assertTrue(v.calls.contains("onEnrichment(evt,e2)"));
        assertTrue(v.calls.contains("onApiEnrichment(evt,e1)"));
        assertTrue(v.calls.contains("onApiEnrichment(evt,e2)"));

        assertTrue(v.calls.contains("onBizLogic(evt,b1)"));
        assertTrue(v.calls.contains("onBizLogic(evt,b2)"));
        assertTrue(v.calls.contains("onCelFileLogic(evt,b1)"));
        assertTrue(v.calls.contains("onCelInlineLogic(evt,b2)"));
    }
}
