package com.hcltech.rmg.xml;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.xml.exceptions.XmlValidationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Engine-agnostic tests for XmlTypeClass implementations.
 * Concrete engines should extend this and only add engine-specific assertions.
 */
public abstract class AbstractXmlTypeClassTest<S> {

    protected final XmlTypeClass<Map<String, Object>, S> eng;

    protected AbstractXmlTypeClassTest(XmlTypeClass<Map<String, Object>, S> eng) {
        this.eng = eng;
        assertNotNull(this.eng, "engine must not be null");
    }

    /**
     * Classpath prefix for test schemas.
     */
    protected String resourcePrefix() {
        return "testxmlloader/";
    }

    /**
     * Schema files (relative to prefix) to load.
     */
    protected List<String> schemaNames() {
        return List.of("a.xsd", "b.xsd");
    }

    /**
     * Full classpath names (prefix + name) for loadSchemas(...)
     */
    private List<String> fullSchemaNames() {
        String pref = resourcePrefix();
        return schemaNames().stream().map(n -> pref + n).collect(Collectors.toList());
    }

    // ---------- helpers ----------

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object o) {
        return (Map<String, Object>) o;
    }

    /**
     * Extracts element text whether stored as {"text": "..."} or a plain String.
     */
    protected static String elementText(Object nodeOrText) {
        if (nodeOrText == null) return null;
        if (nodeOrText instanceof String s) return s;
        if (nodeOrText instanceof Map<?, ?> m) {
            Object t = m.get("text");
            return (t instanceof String) ? (String) t : null;
        }
        return null;
    }

    /**
     * Reads an attribute value from a CEL-friendly node: {"attr":{"id":"..."}...}.
     */
    protected static String attr(Object node, String name) {
        if (!(node instanceof Map<?, ?> m)) return null;
        Object a = m.get("attr");
        if (!(a instanceof Map<?, ?> attrs)) return null;
        Object v = attrs.get(name);
        return (v instanceof String) ? (String) v : null;
    }

    // ---------- general tests (apply to any engine) ----------

    @Test
    void compiles_all_schemas() {
        ErrorsOr<Map<String, S>> eo = XmlTypeClass.loadSchemas(eng, fullSchemaNames());
        assertTrue(eo.isValue(), () -> "Schema load failed: " + eo.getErrors());
        Map<String, S> schemas = eo.valueOrThrow();

        // keys should be the full names we passed in (prefix + file)
        assertEquals(new LinkedHashSet<>(fullSchemaNames()), schemas.keySet());
    }

    @Test
    @DisplayName("parse+validate: happy path against a.xsd")
    void validates_good_xml_against_a() {
        Map<String, S> schemas = XmlTypeClass.loadSchemas(eng, fullSchemaNames()).valueOrThrow();
        S schemaA = schemas.get(resourcePrefix() + "a.xsd");
        assertNotNull(schemaA);

        String good = "<a id=\"A1\"><x>ok</x></a>";
        Map<String, Object> out = eng.parseAndValidate(good, schemaA);

        Map<String, Object> a = asMap(out.get("a"));
        assertNotNull(a, "root element 'a' missing");

        assertEquals("A1", attr(a, "id"));
        assertEquals("ok", elementText(a.get("x")));
    }

    @Test
    @DisplayName("parse+validate: structural violations throw XmlValidationException against a.xsd")
    void rejects_bad_xml_against_a() {
        Map<String, S> schemas = XmlTypeClass.loadSchemas(eng, fullSchemaNames()).valueOrThrow();
        S schemaA = schemas.get(resourcePrefix() + "a.xsd");
        assertNotNull(schemaA);

        assertThrows(XmlValidationException.class,
                () -> eng.parseAndValidate("<a><x>ok</x></a>", schemaA),
                "Expected schema violation for missing 'id'");

        assertThrows(XmlValidationException.class,
                () -> eng.parseAndValidate("<a id=\"A1\"/>", schemaA),
                "Expected schema violation for missing child 'x'");
    }

    @Test
    @DisplayName("parse+validate: b.xsd accepts integer y and throws on non-integer")
    void validates_and_rejects_on_b() {
        Map<String, S> schemas = XmlTypeClass.loadSchemas(eng, fullSchemaNames()).valueOrThrow();
        S schemaB = schemas.get(resourcePrefix() + "b.xsd");
        assertNotNull(schemaB);

        Map<String, Object> ok = eng.parseAndValidate("<b><y>42</y></b>", schemaB);
        Map<String, Object> b = asMap(ok.get("b"));
        assertNotNull(b, "root element 'b' missing");
        assertEquals("42", elementText(b.get("y")));

        assertThrows(XmlValidationException.class,
                () -> eng.parseAndValidate("<b><y>notAnInt</y></b>", schemaB),
                "Expected schema violation for non-integer y");
    }

    @Test
    @DisplayName("engine smoke: parse+validate produces a map with root key")
    void engine_smoke_test_value_roundtrips() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/" + resourcePrefix() + "a.xsd")) {
            assertNotNull(in, "missing a.xsd in " + resourcePrefix());
            S schema = eng.loadSchema("a.xsd", in);

            Map<String, Object> map = eng.parseAndValidate("<a id=\"1\"><x>x</x></a>", schema);

            assertNotNull(map.get("a"));
        }
    }

    @Test
    @DisplayName("invalid XML fails schema validation and throws XmlValidationException (engine-agnostic)")
    void invalid_xml_throws_validation_exception() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/" + resourcePrefix() + "a.xsd")) {
            assertNotNull(in, "missing a.xsd in " + resourcePrefix());
            S schema = eng.loadSchema("a.xsd", in);

            assertThrows(XmlValidationException.class,
                    () -> eng.parseAndValidate("<a id=\"no-child\"/>", schema),
                    "XXE/schema violation should be rejected");
        }
    }

    @Test
    @DisplayName("DOCTYPE/XXE is disallowed (throws XmlValidationException)")
    void xxe_doctype_is_disallowed_and_throws() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/" + resourcePrefix() + "a.xsd")) {
            assertNotNull(in, "missing a.xsd in " + resourcePrefix());
            S schema = eng.loadSchema("a.xsd", in);

            String evil = """
                    <?xml version="1.0"?>
                    <!DOCTYPE a [ <!ENTITY xxe SYSTEM "file:///etc/passwd"> ]>
                    <a id="1"><x>&xxe;</x></a>
                    """;

            assertThrows(XmlValidationException.class,
                    () -> eng.parseAndValidate(evil, schema),
                    "XXE/DOCTYPE should be rejected");
        }
    }

    // -----------------------------
    // real <msg> mapping test
    // -----------------------------

    @Test
    @DisplayName("parse+validate: <msg> schema yields sensible map shape (leafs as text)")
    void parses_msg_schema_sensible_shape() throws Exception {
        // inline XSD that matches your test <msg> payload
        String xsd = """
                <?xml version="1.0" encoding="UTF-8"?>
                <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
                  <xs:element name="msg">
                    <xs:complexType>
                      <xs:sequence>
                        <xs:element name="domainId"  type="xs:string"/>
                        <xs:element name="eventType" type="xs:string" minOccurs="0"/>
                        <xs:element name="count"     type="xs:integer" minOccurs="0"/>
                      </xs:sequence>
                    </xs:complexType>
                  </xs:element>
                </xs:schema>
                """;

        String xml = "<msg><domainId>153</domainId><eventType>test-event</eventType><count>0</count></msg>";

        S schema;
        try (InputStream in = new ByteArrayInputStream(xsd.getBytes(StandardCharsets.UTF_8))) {
            schema = eng.loadSchema("msg.xsd", in);
        }
        Map<String, Object> out = eng.parseAndValidate(xml, schema);
        assertTrue(out.containsKey("msg"), "root should contain 'msg' key");
        Map<String, Object> msg = asMap(out.get("msg"));

        assertEquals("153", elementText(msg.get("domainId")));
        assertEquals("test-event", elementText(msg.get("eventType")));
        assertEquals("0", elementText(msg.get("count")));

        // ensure no double-nesting under leafs (either String or {"text":...})
        Object evt = msg.get("eventType");
        if (evt instanceof Map<?, ?> m) {
            // acceptable only if it's exactly {"text": "..."}
            assertTrue(m.containsKey("text") && m.size() == 1,
                    "Leaf node should be String or {\"text\":...}, not nested element name again");
        } else {
            assertTrue(evt instanceof String, "Leaf should be plain String");
        }
    }

    // -----------------------------
    // extractId(...) test cases
    // -----------------------------

    @Test
    @DisplayName("extractId: happy path with simple path list")
    void extractId_happy_path_with_simple_path() {
        String xml = """
                <Envelope>
                  <Body>
                    <Order>
                      <Id>ABC-123</Id>
                    </Order>
                  </Body>
                </Envelope>
                """;

        ErrorsOr<String> id = eng.extractId(xml, List.of("Envelope", "Body", "Order", "Id"));
        assertTrue(id.isValue(), "Expected id to be extracted");
        assertEquals("ABC-123", id.valueOrThrow());
    }

    @Test
    @DisplayName("extractId: returns error when path not found")
    void extractId_returns_error_when_not_found() {
        String xml = "<root><child>v</child></root>";
        ErrorsOr<String> id = eng.extractId(xml, List.of("root", "missing"));

        assertTrue(id.isError(), "Expected error when id path not found");
        assertTrue(id.errorsOrThrow().get(0).toLowerCase().contains("key"),
                "Message should indicate missing key path");
    }

    @Test
    @DisplayName("extractId: returns error when element text is empty/blank")
    void extractId_returns_error_when_empty_text() {
        String xml = "<root><id>   </id></root>";
        ErrorsOr<String> id = eng.extractId(xml, List.of("root", "id"));

        assertTrue(id.isError(), "Expected error for empty id element");
    }

    // -----------------------------
// XmlTypeClass helper tests (classpath schema loading)
// -----------------------------

    @Test
    @DisplayName("loadOptionalSchema: null/blank → empty map")
    void loadOptionalSchema_null_or_blank_returns_empty() {
        var e1 = XmlTypeClass.loadOptionalSchema(eng, null);
        var e2 = XmlTypeClass.loadOptionalSchema(eng, "   ");
        assertTrue(e1.isValue() && e1.valueOrThrow().isEmpty());
        assertTrue(e2.isValue() && e2.valueOrThrow().isEmpty());
    }

    @Test
    @DisplayName("loadOptionalSchema: not found → error with path")
    void loadOptionalSchema_not_found_returns_error() {
        String missing = resourcePrefix() + "nope-does-not-exist.xsd";
        var eo = XmlTypeClass.loadOptionalSchema(eng, missing);
        assertTrue(eo.isError());
        assertTrue(eo.errorsOrThrow().get(0).contains("Schema resource not found on classpath: " + missing));
    }

    @Test
    @DisplayName("loadOptionalSchema: success → single-entry map {name→schema}")
    void loadOptionalSchema_success_returns_singleton() {
        String name = resourcePrefix() + "a.xsd";
        var eo = XmlTypeClass.loadOptionalSchema(eng, name);
        assertTrue(eo.isValue(), () -> "Load failed: " + eo.getErrors());
        var m = eo.valueOrThrow();
        assertEquals(1, m.size());
        assertTrue(m.containsKey(name));
        assertNotNull(m.get(name));
    }
    @Test
    @DisplayName("loadSchemas: mixed list → collects all errors (blank, missing, duplicate)")
    void loadSchemas_mixed_collects_errors() {
        String ok = resourcePrefix() + "a.xsd";
        String missing = resourcePrefix() + "missing.xsd";

        var names = java.util.Arrays.asList(
                null,          // blank → error
                "   ",         // blank → error
                ok,            // first OK
                missing,       // not found → error
                ok             // duplicate → error
        );

        var eo = XmlTypeClass.loadSchemas(eng, names);
        assertTrue(eo.isError(), () -> "Expected errors but got value: " + eo.valueOrThrow());

        var errs = eo.errorsOrThrow();
        assertTrue(errs.stream().anyMatch(s -> s.equals("Schema name must be non-empty")),
                "Should report blank name");
        assertTrue(errs.stream().anyMatch(s -> s.contains("Schema resource not found on classpath: " + missing)),
                "Should report missing resource");
        assertTrue(errs.stream().anyMatch(s -> s.equals("Duplicate schema name: " + ok)),
                "Should report duplicate schema name");
    }

    @Test
    @DisplayName("loadSchemas: all OK → returns immutable copy map in input order")
    void loadSchemas_success_returns_map() {
        String a = resourcePrefix() + "a.xsd";
        String b = resourcePrefix() + "b.xsd";
        var eo = XmlTypeClass.loadSchemas(eng, List.of(a, b));
        assertTrue(eo.isValue(), () -> "Schema load failed: " + eo.getErrors());

        Map<String, ?> m = eo.valueOrThrow();
        assertEquals(2, m.size());
        assertTrue(m.containsKey(a) && m.containsKey(b));
        assertNotNull(m.get(a));
        assertNotNull(m.get(b));

        // Map.copyOf(...) should make it unmodifiable
        assertThrows(UnsupportedOperationException.class, () -> m.put("x", null));
    }

}
