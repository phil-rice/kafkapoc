package com.hcltech.rmg.xml;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public abstract class AbstractXmlTypeClassTest<S> {

    protected final XmlTypeClass<S> eng;

    protected AbstractXmlTypeClassTest(XmlTypeClass<S> eng) {
        this.eng = eng;
        assertNotNull(this.eng, "engine must not be null");
    }

    /** Classpath prefix for test schemas. */
    protected String resourcePrefix() { return "testxmlloader/"; }

    /** Schema files to load from the prefix. */
    protected List<String> schemaNames() { return List.of("a.xsd", "b.xsd"); }

    // ---------- helpers ----------

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object o) {
        return (Map<String, Object>) o;
    }

    /**
     * Extracts element text whether it's stored as a map {"text": "..."} or as a plain String.
     */
    private static String elementText(Object nodeOrText) {
        if (nodeOrText == null) return null;
        if (nodeOrText instanceof String s) return s;
        if (nodeOrText instanceof Map<?,?> m) {
            Object t = m.get("text");
            return (t instanceof String) ? (String) t : null;
        }
        return null;
    }

    /**
     * Reads an attribute value from a CEL-friendly node: {"attr": {"id": "...", ...}, ...}
     */
    private static String attr(Object node, String name) {
        if (!(node instanceof Map<?,?> m)) return null;
        Object a = m.get("attr");
        if (!(a instanceof Map<?,?> attrs)) return null;
        Object v = attrs.get(name);
        return (v instanceof String) ? (String) v : null;
    }

    // ---------- tests ----------

    @Test
    void compiles_all_schemas() {
        Map<String, S> schemas = XmlTypeClass.loadSchemas(eng, resourcePrefix(), schemaNames());
        assertEquals(new LinkedHashSet<>(schemaNames()), schemas.keySet());
    }

    @Test
    void validates_good_xml_against_a() {
        Map<String, S> schemas = XmlTypeClass.loadSchemas(eng, resourcePrefix(), schemaNames());
        S schemaA = schemas.get("a.xsd");
        assertNotNull(schemaA);

        String good = "<a id=\"A1\"><x>ok</x></a>";
        Map<String,Object> out = eng.parseAndValidate(good, schemaA);

        Map<String,Object> a = asMap(out.get("a"));
        assertNotNull(a, "root element 'a' missing");

        // id attribute
        assertEquals("A1", attr(a, "id"));

        // child x text (accept map-with-text or plain string)
        assertEquals("ok", elementText(a.get("x")));
    }

    @Test
    void rejects_bad_xml_against_a() {
        Map<String, S> schemas = XmlTypeClass.loadSchemas(eng, resourcePrefix(), schemaNames());
        S schemaA = schemas.get("a.xsd");
        assertNotNull(schemaA);

        String missingAttr = "<a><x>ok</x></a>"; // 'id' required
        assertThrows(RuntimeException.class, () -> eng.parseAndValidate(missingAttr, schemaA));

        String missingChild = "<a id=\"A1\"/>";  // 'x' required
        assertThrows(RuntimeException.class, () -> eng.parseAndValidate(missingChild, schemaA));
    }

    @Test
    void validates_and_rejects_on_b() {
        Map<String, S> schemas = XmlTypeClass.loadSchemas(eng, resourcePrefix(), schemaNames());
        S schemaB = schemas.get("b.xsd");
        assertNotNull(schemaB);

        // good: <b><y>42</y></b>
        Map<String,Object> ok = eng.parseAndValidate("<b><y>42</y></b>", schemaB);
        Map<String,Object> b = asMap(ok.get("b"));
        assertNotNull(b, "root element 'b' missing");
        assertEquals("42", elementText(b.get("y")));

        // bad: non-int content
        assertThrows(RuntimeException.class, () -> eng.parseAndValidate("<b><y>notAnInt</y></b>", schemaB));
    }
}
