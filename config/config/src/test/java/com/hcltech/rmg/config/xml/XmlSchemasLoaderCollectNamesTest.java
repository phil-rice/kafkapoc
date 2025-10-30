package com.hcltech.rmg.config.xml;

import com.hcltech.rmg.config.aspect.AspectMap;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.fixture.ConfigTestFixture;
import com.hcltech.rmg.config.transformation.XsltTransform;
import org.junit.jupiter.api.Test;

import javax.xml.validation.Schema;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class XmlSchemasLoaderCollectNamesTest {

    private static final String PREFIX = "testschemas/"; // resources root for the test XSDs

    // ----------------------------
    // collectSchemaNames tests
    // ----------------------------

    @Test
    void collect_returns_empty_when_no_xslt_transforms() {
        BehaviorConfig config = ConfigTestFixture.configWith(
                "evt",
                new AspectMap(ConfigTestFixture.v(), ConfigTestFixture.t(), ConfigTestFixture.e(), ConfigTestFixture.b())
        );

        Set<String> names = XmlSchemasLoader.collectSchemaNames(config);

        assertEquals(Set.of(), names);
    }

    @Test
    void collect_from_single_event_multiple_transforms_trims_and_preserves_order() {
        var am = new AspectMap(
                ConfigTestFixture.v(),
                ConfigTestFixture.t(
                        ConfigTestFixture.kv("xsltA", new XsltTransform("a.xslt", "a.xsd")),
                        ConfigTestFixture.kv("xsltB", new XsltTransform("b.xslt", "  b.xsd  ")) // should be trimmed
                ),
                ConfigTestFixture.e(),
                ConfigTestFixture.b()
        );
        BehaviorConfig config = ConfigTestFixture.configWith("evt", am);

        Set<String> names = XmlSchemasLoader.collectSchemaNames(config);

        Set<String> expected = new LinkedHashSet<>();
        expected.add("a.xsd");
        expected.add("b.xsd");
        assertEquals(expected, names);
    }

    @Test
    void collect_de_dupes_across_events_preserving_first_seen_order() {
        var evt1 = new AspectMap(
                ConfigTestFixture.v(),
                ConfigTestFixture.t(
                        ConfigTestFixture.kv("t1", new XsltTransform("one.xslt", "one.xsd")),
                        ConfigTestFixture.kv("t2", new XsltTransform("two.xslt", "two.xsd"))
                ),
                ConfigTestFixture.e(), ConfigTestFixture.b()
        );
        var evt2 = new AspectMap(
                ConfigTestFixture.v(),
                ConfigTestFixture.t(
                        ConfigTestFixture.kv("t3", new XsltTransform("two-again.xslt", "two.xsd")),
                        ConfigTestFixture.kv("t4", new XsltTransform("three.xslt", "three.xsd"))
                ),
                ConfigTestFixture.e(), ConfigTestFixture.b()
        );
        BehaviorConfig config = new BehaviorConfig(Map.of("alpha", evt1, "beta", evt2));

        Set<String> names = XmlSchemasLoader.collectSchemaNames(config);

        Set<String> expected = new LinkedHashSet<>();
        expected.add("one.xsd");
        expected.add("two.xsd");
        expected.add("three.xsd");
        assertEquals(expected, names);
    }

    @Test
    void collect_throws_if_transform_schema_missing_or_blank() {
        var am = new AspectMap(
                ConfigTestFixture.v(),
                ConfigTestFixture.t(
                        // only the blank case; null triggers NPE in XsltTransform ctor
                        ConfigTestFixture.kv("xsltBad", new XsltTransform("bad.xslt", "   "))
                ),
                ConfigTestFixture.e(),
                ConfigTestFixture.b()
        );
        BehaviorConfig cfg = ConfigTestFixture.configWith("evt", am);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> XmlSchemasLoader.collectSchemaNames(cfg));
        assertTrue(ex.getMessage().contains("Missing schema"),
                "Expected a 'Missing schema' message, got: " + ex.getMessage());
    }

    // ----------------------------
    // compileSchemas / loadSchemas tests
    // ----------------------------

    @Test
    void compile_returns_empty_for_null_or_empty_names() {
        assertEquals(Map.of(), XmlSchemasLoader.compileSchemas(PREFIX, null, 0));
        assertEquals(Map.of(), XmlSchemasLoader.compileSchemas(PREFIX, List.of(), 0));
    }

    @Test
    void compile_builds_simple_schema() {
        Map<String, Schema> out = XmlSchemasLoader.compileSchemas(PREFIX, List.of("a.xsd"), 0);
        assertEquals(Set.of("a.xsd"), out.keySet());
        assertNotNull(out.get("a.xsd"));
    }

    @Test
    void compile_resolves_relative_includes_via_systemId() {
        // parent.xsd includes nested/child.xsd with a relative path
        Map<String, Schema> out = XmlSchemasLoader.compileSchemas(PREFIX, List.of("parent.xsd"), 0);
        assertEquals(Set.of("parent.xsd"), out.keySet());
        assertNotNull(out.get("parent.xsd"));
    }

    @Test
    void loadSchemas_stitches_collect_then_compile_auto_parallel() {
        var am = new AspectMap(
                ConfigTestFixture.v(),
                ConfigTestFixture.t(
                        ConfigTestFixture.kv("t1", new XsltTransform("t1.xslt", "a.xsd")),
                        ConfigTestFixture.kv("t2", new XsltTransform("t2.xslt", "parent.xsd"))
                ),
                ConfigTestFixture.e(),
                ConfigTestFixture.b()
        );
        BehaviorConfig cfg = new BehaviorConfig(Map.of("evt", am));

        Map<String, Schema> out = XmlSchemasLoader.loadSchemas(cfg, PREFIX); // auto parallelism
        assertEquals(new LinkedHashSet<>(List.of("a.xsd", "parent.xsd")), out.keySet());
        assertNotNull(out.get("a.xsd"));
        assertNotNull(out.get("parent.xsd"));
    }

    @Test
    void loadSchemas_overload_with_fixed_parallelism() {
        var am = new AspectMap(
                ConfigTestFixture.v(),
                ConfigTestFixture.t(
                        ConfigTestFixture.kv("t", new XsltTransform("t.xslt", "a.xsd"))
                ),
                ConfigTestFixture.e(),
                ConfigTestFixture.b()
        );
        BehaviorConfig cfg = new BehaviorConfig(Map.of("evt", am));

        Map<String, Schema> out = XmlSchemasLoader.loadSchemas(cfg, PREFIX, 2);
        assertEquals(Set.of("a.xsd"), out.keySet());
        assertNotNull(out.get("a.xsd"));
    }

    @Test
    void compile_rejects_http_includes_due_to_security_hardening() {
        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                XmlSchemasLoader.compileSchemas(PREFIX, List.of("parent-http.xsd"), 0)
        );

        // Must mention the failing resource (outer wrapper)
        assertTrue(ex.toString().contains("parent-http.xsd"),
                "Message should mention the failing resource: " + ex);

        // Walk the cause chain and accept any strong signal of a blocked external fetch.
        assertTrue(hasSecuritySignal(ex),
                "Expected failure due to blocked external include (HTTP/external), got: " + flatten(ex));
    }

    private static boolean hasSecuritySignal(Throwable t) {
        while (t != null) {
            String m = String.valueOf(t.getMessage()).toLowerCase();
            if (m.contains("http://") ||
                    m.contains("https://") ||
                    m.contains("external") ||
                    m.contains("schema_reference") ||
                    m.contains("accessexternalschema")) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    private static String flatten(Throwable t) {
        StringBuilder sb = new StringBuilder(t.getClass().getName()).append(": ").append(t.getMessage());
        Throwable c = t.getCause();
        while (c != null) {
            sb.append(" -> ").append(c.getClass().getName()).append(": ").append(c.getMessage());
            c = c.getCause();
        }
        return sb.toString();
    }

}
