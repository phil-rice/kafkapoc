package com.hcltech.rmg.config.fixture;

import com.hcltech.rmg.config.aspect.AspectMap;
import com.hcltech.rmg.config.bizlogic.BizLogicAspect;
import com.hcltech.rmg.config.bizlogic.CelFileLogic;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.config.BehaviorConfigVisitor;
import com.hcltech.rmg.config.enrich.ApiEnrichment;
import com.hcltech.rmg.config.enrich.EnrichmentAspect;
import com.hcltech.rmg.config.transformation.TransformationAspect;
import com.hcltech.rmg.config.transformation.XsltTransform;
import com.hcltech.rmg.config.validation.CelValidation;
import com.hcltech.rmg.config.validation.ValidationAspect;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Map.entry;

/** Test-only helpers to keep config tests terse and intention-revealing. */
public final class ConfigTestFixture {

    private ConfigTestFixture() {}

    /* ------------ I/O helpers ------------ */

    public static InputStream resourceStream(String path) {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
        if (in == null) throw new IllegalArgumentException("Test resource not found: " + path);
        return in;
    }

    /* ------------ Tiny entry helper ------------ */

    public static <T> Map.Entry<String, T> kv(String key, T value) {
        return entry(key, value);
    }

    /* ------------ Shorthand map builders for AspectMap ctor (module-name aware) ------------ */

    // ----- Validation maps -----
    public static Map<String, ValidationAspect> v(Map<String, ValidationAspect> m) {
        return (m == null || m.isEmpty()) ? Map.of() : new LinkedHashMap<>(m);
    }
    @SafeVarargs
    public static Map<String, ValidationAspect> v(Map.Entry<String, ValidationAspect>... entries) {
        if (entries == null || entries.length == 0) return Map.of();
        Map<String, ValidationAspect> m = new LinkedHashMap<>();
        for (var e : entries) m.put(e.getKey(), e.getValue());
        return m;
    }

    // ----- Transformation maps -----
    public static Map<String, TransformationAspect> t(Map<String, TransformationAspect> m) {
        return (m == null || m.isEmpty()) ? Map.of() : new LinkedHashMap<>(m);
    }
    @SafeVarargs
    public static Map<String, TransformationAspect> t(Map.Entry<String, TransformationAspect>... entries) {
        if (entries == null || entries.length == 0) return Map.of();
        Map<String, TransformationAspect> m = new LinkedHashMap<>();
        for (var e : entries) m.put(e.getKey(), e.getValue());
        return m;
    }

    // ----- Enrichment maps -----
    public static Map<String, EnrichmentAspect> e(Map<String, EnrichmentAspect> m) {
        return (m == null || m.isEmpty()) ? Map.of() : new LinkedHashMap<>(m);
    }
    @SafeVarargs
    public static Map<String, EnrichmentAspect> e(Map.Entry<String, EnrichmentAspect>... entries) {
        if (entries == null || entries.length == 0) return Map.of();
        Map<String, EnrichmentAspect> m = new LinkedHashMap<>();
        for (var e : entries) m.put(e.getKey(), e.getValue());
        return m;
    }

    // ----- BizLogic maps -----
    public static Map<String, BizLogicAspect> b(Map<String, BizLogicAspect> m) {
        return (m == null || m.isEmpty()) ? Map.of() : new LinkedHashMap<>(m);
    }
    @SafeVarargs
    public static Map<String, BizLogicAspect> b(Map.Entry<String, BizLogicAspect>... entries) {
        if (entries == null || entries.length == 0) return Map.of();
        Map<String, BizLogicAspect> m = new LinkedHashMap<>();
        for (var e : entries) m.put(e.getKey(), e.getValue());
        return m;
    }

    /* ------------ Prebaked mini-fixtures ------------ */

    public static BehaviorConfig minimalConfig() {
        return new BehaviorConfig(Map.of());
    }

    public static BehaviorConfig complexExpected() {
        AspectMap expectedAspectMap = new AspectMap(
                v(kv("notification", new CelValidation("somecel"))),
                t(kv("notification", new XsltTransform("xforms/ready.xslt", "schemas/ready.xml"))),
                e(kv("notification", new ApiEnrichment("getRecipient", Map.of("id", "${inp.recipientId}")))),
                b(kv("notification", new CelFileLogic("cel/ready.cel")))
        );
        return new BehaviorConfig(Map.ofEntries(entry("readyForDelivery", expectedAspectMap)));
    }

    public static AspectMap fullAspectMapExample() {
        return new AspectMap(
                v(
                        kv("xml", new CelValidation("somecel"))
                ),
                t(kv("xslt", new XsltTransform("transform.xslt", "transform.xsd"))),
                e(kv("api", new ApiEnrichment("http://example", Map.of("q", "1")))),
                b(
                        kv("notification", new CelFileLogic("logic.cel")),
                        kv("rules", new CelInlineLogic("a + b"))
                )
        );
    }

    public static BehaviorConfig configWith(String eventName, AspectMap map) {
        return new BehaviorConfig(Map.of(eventName, map));
    }

    /* ------------ Visitor & assertions support ------------ */

    /** Recording visitor used across tests. */
    public static final class RecordingVisitorBehavior implements BehaviorConfigVisitor {
        public final List<String> calls = new ArrayList<>();

        private void hit(String m) { calls.add(m); }

        @Override public void onConfig(BehaviorConfig config) { hit("onConfig"); }

        @Override public void onEvent(String eventName, AspectMap aspects) {
            hit("onEvent(" + eventName + ")");
        }

        @Override public void onValidation(String e, String n, ValidationAspect v) {
            hit("onValidation(" + e + "," + n + ")");
        }

        @Override
        public void onCelValidation(String eventName, String name, CelValidation v) {
            hit("onCelValidation(" + eventName + "," + name + ")");
        }

        @Override public void onTransformation(String e, String n, TransformationAspect t) {
            hit("onTransformation(" + e + "," + n + ")");
        }

        @Override public void onXsltTransform(String e, String n, XsltTransform t) {
            hit("onXsltTransform(" + e + "," + n + ")");
        }

        @Override public void onEnrichment(String e, String n, EnrichmentAspect a) {
            hit("onEnrichment(" + e + "," + n + ")");
        }

        @Override public void onApiEnrichment(String e, String n, ApiEnrichment a) {
            hit("onApiEnrichment(" + e + "," + n + ")");
        }

        @Override public void onBizLogic(String e, String n, BizLogicAspect b) {
            hit("onBizLogic(" + e + "," + n + ")");
        }

        @Override public void onCelFileLogic(String e, String n, CelFileLogic b) {
            hit("onCelFileLogic(" + e + "," + n + ")");
        }

        @Override public void onCelInlineLogic(String e, String n, CelInlineLogic b) {
            hit("onCelInlineLogic(" + e + "," + n + ")");
        }
    }

    public static List<String> sorted(List<String> list) {
        return list.stream().sorted().collect(Collectors.toList());
    }
}
