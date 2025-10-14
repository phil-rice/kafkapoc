package com.hcltech.rmg.config.config;

import com.hcltech.rmg.config.aspect.AspectMap;
import com.hcltech.rmg.config.bizlogic.BizLogicAspect;
import com.hcltech.rmg.config.bizlogic.CelFileLogic;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.enrich.EnrichmentAspect;
import com.hcltech.rmg.config.enrich.FixedEnrichment;
import com.hcltech.rmg.config.transformation.TransformationAspect;
import com.hcltech.rmg.config.transformation.XmlTransform;
import com.hcltech.rmg.config.transformation.XsltTransform;
import com.hcltech.rmg.config.validation.CelValidation;
import com.hcltech.rmg.config.validation.ValidationAspect;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for {@link BehaviorConfigWalker} that validate
 * dispatch into {@link BehaviorConfigVisitor}, covering all families and subtypes.
 */
public class BehaviorConfigWalkerTest {

    /**
     * Records calls in order for verification.
     */
    static class RecordingVisitor implements BehaviorConfigVisitor {
        final List<String> calls = new ArrayList<>();

        private void hit(String m) {
            calls.add(m);
        }

        // Root & container
        @Override
        public void onConfig(BehaviorConfig config) {
            hit("onConfig");
        }

        @Override
        public void onEvent(String eventName, AspectMap aspects) {
            hit("onEvent(" + eventName + ")");
        }

        // Validation
        @Override
        public void onValidation(String e, String m, ValidationAspect v) {
            hit("onValidation(" + e + "," + m + ")");
        }

        @Override
        public void onCelValidation(String e, String m, CelValidation v) {
            hit("onCelValidation(" + e + "," + m + ")");
        }

        // Transformation
        @Override
        public void onTransformation(String e, String m, TransformationAspect t) {
            hit("onTransformation(" + e + "," + m + ")");
        }

        @Override
        public void onXmlTransform(String e, String m, XmlTransform t) {
            hit("onXmlTransform(" + e + "," + m + ")");
        }

        @Override
        public void onXsltTransform(String e, String m, XsltTransform t) {
            hit("onXsltTransform(" + e + "," + m + ")");
        }

        // Enrichment
        @Override
        public void onEnrichment(String e, String m, EnrichmentAspect a) {
            hit("onEnrichment(" + e + "," + m + ")");
        }

        @Override
        public void onFixedEnrichment(String e, String m, FixedEnrichment a) {
            hit("onFixedEnrichment(" + e + "," + m + ")");
        }

        // BizLogic
        @Override
        public void onBizLogic(String e, String m, BizLogicAspect b) {
            hit("onBizLogic(" + e + "," + m + ")");
        }

        @Override
        public void onCelFileLogic(String e, String m, CelFileLogic b) {
            hit("onCelFileLogic(" + e + "," + m + ")");
        }

        @Override
        public void onCelInlineLogic(String e, String m, CelInlineLogic b) {
            hit("onCelInlineLogic(" + e + "," + m + ")");
        }
    }

    private static int indexOf(List<String> calls, String needle) {
        int i = calls.indexOf(needle);
        assertTrue(i >= 0, "Expected call not found: " + needle + " in " + calls);
        return i;
    }

    private static void assertBefore(List<String> calls, String first, String second) {
        int a = indexOf(calls, first);
        int b = indexOf(calls, second);
        assertTrue(a < b, "Expected \"" + first + "\" to occur before \"" + second + "\" but got order " + calls);
    }

    private static List<String> sorted(List<String> list) {
        return list.stream().sorted().toList();
    }

    @Test
    void walks_full_graph_and_invokes_generic_then_specific_per_item_including_xml_xslt_and_fixed() {
        var evt = new AspectMap(
                Map.of(
                        "cel", new CelValidation("a + b > 0")
                ),
                Map.of(
                        "xml", new XmlTransform("schema.xsd"),
                        "xslt", new XsltTransform("transform.xslt", "transform.xsd")
                ),
                Map.of(
                        "fixed", new FixedEnrichment(
                                List.of(List.of("addr", "line1"), List.of("addr", "line2")),
                                List.of("addr", "postcode"),
                                Map.of("L1.L2", "PC1")
                        )
                ),
                Map.of(
                        "fileLogic", new CelFileLogic("logic.cel"),
                        "inlineLogic", new CelInlineLogic("a + b")
                )
        );
        var config = new BehaviorConfig(Map.of("orderPlaced", evt));

        var v = new RecordingVisitor();
        BehaviorConfigWalker.walk(config, v);

        // Build the exact expected set of calls (no duplicates, include fixed)
        var expected = List.of(
                // root + event
                "onConfig",
                "onEvent(orderPlaced)",

                // validation
                "onValidation(orderPlaced,cel)",
                "onCelValidation(orderPlaced,cel)",

                // transformation
                "onTransformation(orderPlaced,xml)",
                "onXmlTransform(orderPlaced,xml)",
                "onTransformation(orderPlaced,xslt)",
                "onXsltTransform(orderPlaced,xslt)",

                "onEnrichment(orderPlaced,fixed)",
                "onFixedEnrichment(orderPlaced,fixed)",

                // bizlogic (generic + specific)
                "onBizLogic(orderPlaced,fileLogic)",
                "onCelFileLogic(orderPlaced,fileLogic)",
                "onBizLogic(orderPlaced,inlineLogic)",
                "onCelInlineLogic(orderPlaced,inlineLogic)"
        );

        // Sort both lists and assert exact equality (catches dupes/missing)
        var expectedSorted = new ArrayList<>(expected);
        var actualSorted = new ArrayList<>(v.calls);
        expectedSorted.sort(String::compareTo);
        actualSorted.sort(String::compareTo);
        assertEquals(expectedSorted, actualSorted, "Sorted calls mismatch.\nActual: " + v.calls);

        // Keep order guarantees for each item (generic before specific)
        assertBefore(v.calls, "onValidation(orderPlaced,cel)", "onCelValidation(orderPlaced,cel)");
        assertBefore(v.calls, "onTransformation(orderPlaced,xml)",  "onXmlTransform(orderPlaced,xml)");
        assertBefore(v.calls, "onTransformation(orderPlaced,xslt)","onXsltTransform(orderPlaced,xslt)");
        assertBefore(v.calls, "onEnrichment(orderPlaced,fixed)",   "onFixedEnrichment(orderPlaced,fixed)");
        assertBefore(v.calls, "onBizLogic(orderPlaced,fileLogic)", "onCelFileLogic(orderPlaced,fileLogic)");
        assertBefore(v.calls, "onBizLogic(orderPlaced,inlineLogic)", "onCelInlineLogic(orderPlaced,inlineLogic)");
    }

    @Test
    void tolerates_empty_and_nulls_including_null_values_inside_families() {
        // 1) Null events map
        var cfgNullEvents = new BehaviorConfig(null);
        var v1 = new RecordingVisitor();
        BehaviorConfigWalker.walk(cfgNullEvents, v1);
        assertEquals(List.of("onConfig"), sorted(v1.calls));

        // 2) Empty event with all families empty
        var cfgEmptyEvent = new BehaviorConfig(Map.of("emptyEvent", AspectMap.empty()));
        var v2 = new RecordingVisitor();
        BehaviorConfigWalker.walk(cfgEmptyEvent, v2);
        assertEquals(List.of("onConfig", "onEvent(emptyEvent)"), sorted(v2.calls));

        // 3) Families with null values in their maps should be skipped
        java.util.Map<String, com.hcltech.rmg.config.validation.ValidationAspect> validations =
                new java.util.LinkedHashMap<>();
        validations.put("v1", new CelValidation("true"));
        validations.put("vNull", null); // intentional

        java.util.Map<String, com.hcltech.rmg.config.transformation.TransformationAspect> transformations =
                new java.util.LinkedHashMap<>();
        transformations.put("xml1", new com.hcltech.rmg.config.transformation.XmlTransform("s.xsd"));
        transformations.put("tNull", null); // intentional

        java.util.Map<String, com.hcltech.rmg.config.enrich.EnrichmentAspect> enrichments =
                new java.util.LinkedHashMap<>();
        enrichments.put("e1", new FixedEnrichment(List.of(List.of("p")), List.of("o"), Map.of()));
        enrichments.put("eNull", null); // intentional

        java.util.Map<String, com.hcltech.rmg.config.bizlogic.BizLogicAspect> bizlogics =
                new java.util.LinkedHashMap<>();
        bizlogics.put("b1", new CelFileLogic("f.cel"));
        bizlogics.put("bNull", null); // intentional

        var evtWithNulls = new AspectMap(validations, transformations, enrichments, bizlogics);
        var cfgWithNulls = new BehaviorConfig(java.util.Map.of("evt", evtWithNulls));
        var v3 = new RecordingVisitor();
        BehaviorConfigWalker.walk(cfgWithNulls, v3);

        // Should contain only non-null hooks
        assertTrue(v3.calls.contains("onEvent(evt)"));

        assertTrue(v3.calls.contains("onValidation(evt,v1)"));
        assertFalse(v3.calls.stream().anyMatch(s -> s.contains("vNull")));

        assertTrue(v3.calls.contains("onTransformation(evt,xml1)"));
        assertTrue(v3.calls.contains("onXmlTransform(evt,xml1)"));
        assertFalse(v3.calls.stream().anyMatch(s -> s.contains("tNull")));

        assertTrue(v3.calls.contains("onEnrichment(evt,e1)"));
        assertTrue(v3.calls.contains("onFixedEnrichment(evt,e1)"));
        assertFalse(v3.calls.stream().anyMatch(s -> s.contains("eNull")));

        assertTrue(v3.calls.contains("onBizLogic(evt,b1)"));
        assertTrue(v3.calls.contains("onCelFileLogic(evt,b1)"));
        assertFalse(v3.calls.stream().anyMatch(s -> s.contains("bNull")));
    }


    /**
     * Helper: build a LinkedHashMap allowing null values.
     */
    @SafeVarargs
    private static <K, V> Map<K, V> mapWithNulls(Object... kv) {
        if (kv.length % 2 != 0) throw new IllegalArgumentException("Expected even number of kv args");
        var m = new java.util.LinkedHashMap<K, V>();
        for (int i = 0; i < kv.length; i += 2) {
            @SuppressWarnings("unchecked") K k = (K) kv[i];
            @SuppressWarnings("unchecked") V v = (V) kv[i + 1];
            m.put(k, v); // allows null values
        }
        return m;
    }

    @Test
    void families_produce_expected_hooks_for_multiple_entries() {
        var evt = new AspectMap(
                Map.of(
                        "v1", new CelValidation("1 > 0"),
                        "v2", new CelValidation("2 > 0")
                ),
                Map.of(
                        "tXml", new XmlTransform("a.xsd"),
                        "tXslt", new XsltTransform("a.xslt", "a.xsd")
                ),
                Map.of(
                        "e2", new FixedEnrichment(List.of(List.of("p2")), List.of("o2"), Map.of()),
                        "e1", new FixedEnrichment(List.of(List.of("p1")), List.of("o1"), Map.of())),
                Map.of(
                        "b2", new CelInlineLogic("x"),
                        "b1", new CelFileLogic("f1")
                )
        );

        var config = new BehaviorConfig(Map.of("evt", evt));
        var v = new RecordingVisitor();

        BehaviorConfigWalker.walk(config, v);

        var expected = List.of(
                "onBizLogic(evt,b1)", "onBizLogic(evt,b2)",    // <- fix case here
                "onCelFileLogic(evt,b1)", "onCelInlineLogic(evt,b2)",
                "onCelValidation(evt,v1)", "onCelValidation(evt,v2)",
                "onConfig",
                "onEnrichment(evt,e1)", "onEnrichment(evt,e2)",
                "onEvent(evt)",
                "onFixedEnrichment(evt,e1)", "onFixedEnrichment(evt,e2)",
                "onTransformation(evt,tXml)", "onTransformation(evt,tXslt)",
                "onValidation(evt,v1)", "onValidation(evt,v2)",
                "onXmlTransform(evt,tXml)", "onXsltTransform(evt,tXslt)"
        );


        // ✅ sort both before asserting equality
        var expectedSorted = new ArrayList<>(expected);
        var actualSorted = new ArrayList<>(v.calls);

        expectedSorted.sort(String::compareTo);
        actualSorted.sort(String::compareTo);

        assertEquals(expectedSorted, actualSorted);
    }

    @Test
    void supports_multiple_events_and_invokes_onEvent_for_each() {
        var evt1 = new AspectMap(
                Map.of("v", new CelValidation("true")),
                Map.of("xml", new XmlTransform("s.xsd")),
                null,
                null
        );
        var evt2 = new AspectMap(
                null,
                Map.of("xslt", new XsltTransform("t.xslt", "s.xsd")),
                Map.of("fixed", new FixedEnrichment(List.of(List.of("p")), List.of("o"), Map.of())),
                Map.of("b", new CelInlineLogic("x"))
        );
        var config = new BehaviorConfig(Map.of(
                "evtA", evt1,
                "evtB", evt2
        ));

        var v = new RecordingVisitor();
        BehaviorConfigWalker.walk(config, v);

        assertTrue(v.calls.contains("onEvent(evtA)"));
        assertTrue(v.calls.contains("onEvent(evtB)"));

        // Spot-check a couple per event
        assertTrue(v.calls.contains("onValidation(evtA,v)"));
        assertTrue(v.calls.contains("onXmlTransform(evtA,xml)"));
        assertTrue(v.calls.contains("onXsltTransform(evtB,xslt)"));
        assertTrue(v.calls.contains("onFixedEnrichment(evtB,fixed)"));
        assertTrue(v.calls.contains("onCelInlineLogic(evtB,b)"));
    }
}
