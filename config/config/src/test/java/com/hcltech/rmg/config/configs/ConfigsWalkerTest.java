package com.hcltech.rmg.config.configs;

import com.hcltech.rmg.config.aspect.AspectMap;
import com.hcltech.rmg.config.bizlogic.BizLogicAspect;
import com.hcltech.rmg.config.bizlogic.CelFileLogic;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.enrich.*;
import com.hcltech.rmg.config.transformation.TransformationAspect;
import com.hcltech.rmg.config.transformation.XmlTransform;
import com.hcltech.rmg.config.transformation.XsltTransform;
import com.hcltech.rmg.config.validation.CelValidation;
import com.hcltech.rmg.config.validation.ValidationAspect;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ConfigsWalker} that validate dispatch into {@link ConfigsVisitor}.
 * Uses only enrichment subtypes with known constructors (MapLookup, Fixed, Csv, Api).
 */
public class ConfigsWalkerTest {

    /* ---------- helpers ---------- */

    /** Construct a minimal Config with only BehaviorConfig populated. */
    private static Config cfg(BehaviorConfig b) {
        // Matches your record's 3-arg ctor: (BehaviorConfig, ParameterConfig, String xmlSchemaPath)
        return new Config(b, null, null);
    }

    /** Construct Configs from a map. Adjust if your Configs class differs. */
    private static Configs configs(Map<String, Config> m) {
        return new Configs(m);
    }

    /** Visitor that records calls (prefixed with paramKey). */
    static class RecordingVisitor implements ConfigsVisitor {
        final List<String> calls = new ArrayList<>();
        private void hit(String s) { calls.add(s); }

        // lifecycle
        @Override public void onConfigStart(String k, Config c) { hit(k + ":onConfigStart"); }
        @Override public void onConfigEnd(String k, Config c) { hit(k + ":onConfigEnd"); }

        // validation
        @Override public void onValidation(String k, String e, String m, ValidationAspect v) { hit(k + ":onValidation(" + e + "," + m + ")"); }
        @Override public void onCelValidation(String k, String e, String m, CelValidation v) { hit(k + ":onCelValidation(" + e + "," + m + ")"); }

        // transformation
        @Override public void onTransformation(String k, String e, String m, TransformationAspect t) { hit(k + ":onTransformation(" + e + "," + m + ")"); }
        @Override public void onXmlTransform(String k, String e, String m, XmlTransform t)           { hit(k + ":onXmlTransform(" + e + "," + m + ")"); }
        @Override public void onXsltTransform(String k, String e, String m, XsltTransform t)         { hit(k + ":onXsltTransform(" + e + "," + m + ")"); }

        // enrichment: generic + specific
        @Override public void onEnrichment(String k, String e, String m, EnrichmentAspect a)                 { hit(k + ":onEnrichment(" + e + "," + m + "," + a.getClass().getSimpleName() + ")"); }
        @Override public void onMapLookupEnrichment(String k, String e, String m, MapLookupEnrichment a)     { hit(k + ":onMapLookupEnrichment(" + e + "," + m + ")"); }
        @Override public void onFixedEnrichment(String k, String e, String m, FixedEnrichment a)             { hit(k + ":onFixedEnrichment(" + e + "," + m + ")"); }
        @Override public void onCsvEnrichment(String k, String e, String m, CsvEnrichment a)                 { hit(k + ":onCsvEnrichment(" + e + "," + m + ")"); }
        @Override public void onApiEnrichment(String k, String e, String m, ApiEnrichment a)                 { hit(k + ":onApiEnrichment(" + e + "," + m + ")"); }
        // (hooks exist for CsvFromAzureEnrichment and CompositeExecutor but we don’t instantiate them here)

        // bizlogic
        @Override public void onBizLogic(String k, String e, String m, BizLogicAspect b)       { hit(k + ":onBizLogic(" + e + "," + m + ")"); }
        @Override public void onCelFileLogic(String k, String e, String m, CelFileLogic b)     { hit(k + ":onCelFileLogic(" + e + "," + m + ")"); }
        @Override public void onCelInlineLogic(String k, String e, String m, CelInlineLogic b) { hit(k + ":onCelInlineLogic(" + e + "," + m + ")"); }
    }

    private static int indexOf(List<String> calls, String needle) {
        int i = calls.indexOf(needle);
        assertTrue(i >= 0, "Missing: " + needle + " in " + calls);
        return i;
    }
    private static void assertBefore(List<String> calls, String a, String b) {
        assertTrue(indexOf(calls, a) < indexOf(calls, b), "Expected \"" + a + "\" before \"" + b + "\"; got " + calls);
    }

    /* ---------- tests ---------- */

    @Test
    void walks_two_paramkeys_and_orders_generic_then_specific_for_known_enrichments() {
        // Build one event containing all families + four enrichment subtypes we can instantiate
        var event = new AspectMap(
                Map.of(
                        "cel", new CelValidation("a > 0")
                ),
                Map.of(
                        "xml", new XmlTransform("schema.xsd"),
                        "xslt", new XsltTransform("t.xslt", "schema.xsd")
                ),
                Map.of(
                        "lookup", new MapLookupEnrichment(List.of(List.of("in", "path")), List.of("out"), Map.of("k", "v")),
                        "fixed", new FixedEnrichment(List.of(List.of("inA")), List.of("outA"), Map.of("k", 1)),
                        "csv", new CsvEnrichment(
                                List.of(List.of("i1")), List.of("o1"),
                                "file.csv",
                                List.of("inCol1"),
                                List.of("outCol1"),
                                "."
                        ),
                        "api", new ApiEnrichment(
                                List.of(List.of("i2")), List.of("o2"),
                                "https://example.test/lookup", "q",
                                1000, 2000
                        )
                ),
                Map.of(
                        "file", new CelFileLogic("logic.cel"),
                        "inline", new CelInlineLogic("a + b")
                )
        );

        var bcA = new BehaviorConfig(Map.of("orderPlaced", event));
        var bcB = new BehaviorConfig(Map.of("orderPlaced", event));
        var cfgs = configs(Map.of(
                "A-key", cfg(bcA),
                "B-key", cfg(bcB)
        ));

        var v = new RecordingVisitor();
        ConfigsWalker.walk(cfgs, v);

        for (String key : List.of("A-key", "B-key")) {
            // validation
            assertBefore(v.calls, key + ":onValidation(orderPlaced,cel)", key + ":onCelValidation(orderPlaced,cel)");

            // transformation
            assertBefore(v.calls, key + ":onTransformation(orderPlaced,xml)",  key + ":onXmlTransform(orderPlaced,xml)");
            assertBefore(v.calls, key + ":onTransformation(orderPlaced,xslt)", key + ":onXsltTransform(orderPlaced,xslt)");

            // enrichments: generic then specific
            assertBefore(v.calls, key + ":onEnrichment(orderPlaced,lookup,MapLookupEnrichment)", key + ":onMapLookupEnrichment(orderPlaced,lookup)");
            assertBefore(v.calls, key + ":onEnrichment(orderPlaced,fixed,FixedEnrichment)",       key + ":onFixedEnrichment(orderPlaced,fixed)");
            assertBefore(v.calls, key + ":onEnrichment(orderPlaced,csv,CsvEnrichment)",           key + ":onCsvEnrichment(orderPlaced,csv)");
            assertBefore(v.calls, key + ":onEnrichment(orderPlaced,api,ApiEnrichment)",           key + ":onApiEnrichment(orderPlaced,api)");

            // bizlogic
            assertBefore(v.calls, key + ":onBizLogic(orderPlaced,file)",   key + ":onCelFileLogic(orderPlaced,file)");
            assertBefore(v.calls, key + ":onBizLogic(orderPlaced,inline)", key + ":onCelInlineLogic(orderPlaced,inline)");
        }

        // lifecycle present per key
        assertTrue(v.calls.contains("A-key:onConfigStart"));
        assertTrue(v.calls.contains("A-key:onConfigEnd"));
        assertTrue(v.calls.contains("B-key:onConfigStart"));
        assertTrue(v.calls.contains("B-key:onConfigEnd"));
    }

    @Test
    void deterministic_sorted_paramkey_order_and_scoping() {
        var evt = new AspectMap(
                Map.of("v", new CelValidation("true")),
                Map.of("xml", new XmlTransform("s.xsd")),
                Map.of("fixed", new FixedEnrichment(List.of(List.of("i")), List.of("o"), Map.of("x", 1))),
                Map.of("b", new CelInlineLogic("x"))
        );
        var bc = new BehaviorConfig(Map.of("evt", evt));

        var map = new LinkedHashMap<String, Config>();
        map.put("zeta", cfg(bc));
        map.put("alpha", cfg(bc));
        map.put("mu", cfg(bc));

        var v = new RecordingVisitor();
        ConfigsWalker.walk(configs(map), v);

        int iAlpha = indexOf(v.calls, "alpha:onConfigStart");
        int iMu    = indexOf(v.calls, "mu:onConfigStart");
        int iZeta  = indexOf(v.calls, "zeta:onConfigStart");
        assertTrue(iAlpha < iMu && iMu < iZeta, "Expected sorted order alpha < mu < zeta");

        // no leakage of paramKey prefixes
        assertTrue(v.calls.stream().anyMatch(s -> s.startsWith("alpha:")));
        assertTrue(v.calls.stream().anyMatch(s -> s.startsWith("mu:")));
        assertTrue(v.calls.stream().anyMatch(s -> s.startsWith("zeta:")));
    }

    @Test
    void tolerates_nulls_and_empties() {
        // null behaviorConfig
        var nullBehaviorCfg = new Config(null, null, null);

        // empty families
        var emptyEvt = AspectMap.empty();
        var emptyCfg = cfg(new BehaviorConfig(Map.of("E", emptyEvt)));

        // families with null members
        Map<String, ValidationAspect> validations = new LinkedHashMap<>();
        validations.put("v1", new CelValidation("true"));
        validations.put("vNull", null);

        Map<String, TransformationAspect> transformations = new LinkedHashMap<>();
        transformations.put("xml1", new XmlTransform("s.xsd"));
        transformations.put("tNull", null);

        Map<String, EnrichmentAspect> enrichments = new LinkedHashMap<>();
        enrichments.put("e1", new MapLookupEnrichment(List.of(List.of("p")), List.of("o"), Map.of()));
        enrichments.put("eNull", null);

        Map<String, BizLogicAspect> bizlogics = new LinkedHashMap<>();
        bizlogics.put("b1", new CelFileLogic("f.cel"));
        bizlogics.put("bNull", null);

        var evtWithNulls = new AspectMap(validations, transformations, enrichments, bizlogics);
        var mixedCfg = cfg(new BehaviorConfig(Map.of("evt", evtWithNulls)));

        var v = new RecordingVisitor();
        ConfigsWalker.walk(configs(Map.of(
                "nullB", nullBehaviorCfg,
                "empty", emptyCfg,
                "mixed", mixedCfg
        )), v);

        // null behavior: only lifecycle
        assertTrue(v.calls.contains("nullB:onConfigStart"));
        assertTrue(v.calls.contains("nullB:onConfigEnd"));

        // empty: lifecycle only
        assertTrue(v.calls.contains("empty:onConfigStart"));
        assertTrue(v.calls.contains("empty:onConfigEnd"));
        assertFalse(v.calls.stream().anyMatch(s -> s.startsWith("empty:onValidation(")));
        assertFalse(v.calls.stream().anyMatch(s -> s.startsWith("empty:onTransformation(")));
        assertFalse(v.calls.stream().anyMatch(s -> s.startsWith("empty:onEnrichment(")));
        assertFalse(v.calls.stream().anyMatch(s -> s.startsWith("empty:onBizLogic(")));

        // mixed: only non-null entries appear
        assertTrue(v.calls.contains("mixed:onValidation(evt,v1)"));
        assertFalse(v.calls.stream().anyMatch(s -> s.contains("vNull")));

        assertTrue(v.calls.contains("mixed:onTransformation(evt,xml1)"));
        assertTrue(v.calls.contains("mixed:onXmlTransform(evt,xml1)"));
        assertFalse(v.calls.stream().anyMatch(s -> s.contains("tNull")));

        assertTrue(v.calls.contains("mixed:onEnrichment(evt,e1,MapLookupEnrichment)"));
        assertTrue(v.calls.contains("mixed:onMapLookupEnrichment(evt,e1)"));
        assertFalse(v.calls.stream().anyMatch(s -> s.contains("eNull")));

        assertTrue(v.calls.contains("mixed:onBizLogic(evt,b1)"));
        assertTrue(v.calls.contains("mixed:onCelFileLogic(evt,b1)"));
        assertFalse(v.calls.stream().anyMatch(s -> s.contains("bNull")));
    }
}
