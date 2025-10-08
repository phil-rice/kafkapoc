package com.hcltech.rmg.xml;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
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
        Map<String, Object> out = eng.parseAndValidate(good, schemaA).valueOrThrow();

        Map<String, Object> a = asMap(out.get("a"));
        assertNotNull(a, "root element 'a' missing");

        assertEquals("A1", attr(a, "id"));
        assertEquals("ok", elementText(a.get("x")));
    }

    @Test
    @DisplayName("parse+validate: structural violations produce ErrorsOr(error) against a.xsd")
    void rejects_bad_xml_against_a() {
        Map<String, S> schemas = XmlTypeClass.loadSchemas(eng, fullSchemaNames()).valueOrThrow();
        S schemaA = schemas.get(resourcePrefix() + "a.xsd");
        assertNotNull(schemaA);

        ErrorsOr<Map<String, Object>> missingAttr =
                eng.parseAndValidate("<a><x>ok</x></a>", schemaA);
        assertTrue(missingAttr.isError(), "Expected schema violation for missing 'id'");
        assertFalse(missingAttr.errorsOrThrow().isEmpty());

        ErrorsOr<Map<String, Object>> missingChild =
                eng.parseAndValidate("<a id=\"A1\"/>", schemaA);
        assertTrue(missingChild.isError(), "Expected schema violation for missing child 'x'");
        assertFalse(missingChild.errorsOrThrow().isEmpty());
    }

    @Test
    @DisplayName("parse+validate: b.xsd accepts integer y and rejects non-integer")
    void validates_and_rejects_on_b() {
        Map<String, S> schemas = XmlTypeClass.loadSchemas(eng, fullSchemaNames()).valueOrThrow();
        S schemaB = schemas.get(resourcePrefix() + "b.xsd");
        assertNotNull(schemaB);

        Map<String, Object> ok = eng.parseAndValidate("<b><y>42</y></b>", schemaB).valueOrThrow();
        Map<String, Object> b = asMap(ok.get("b"));
        assertNotNull(b, "root element 'b' missing");
        assertEquals("42", elementText(b.get("y")));

        ErrorsOr<Map<String, Object>> bad = eng.parseAndValidate("<b><y>notAnInt</y></b>", schemaB);
        assertTrue(bad.isError(), "Expected schema violation for non-integer y");
        assertFalse(bad.errorsOrThrow().isEmpty());
    }

    @Test
    @DisplayName("engine smoke: parse+validate produces a map with root key")
    void engine_smoke_test_value_roundtrips() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/" + resourcePrefix() + "a.xsd")) {
            assertNotNull(in, "missing a.xsd in " + resourcePrefix());
            S schema = eng.loadSchema("a.xsd", in);

            ErrorsOr<Map<String, Object>> eo = eng.parseAndValidate("<a id=\"1\"><x>x</x></a>", schema);
            Map<String, Object> map = eo.valueOrThrow();

            assertNotNull(map.get("a"));
        }
    }

    @Test
    @DisplayName("invalid XML fails schema validation and returns ErrorsOr(error) (engine-agnostic)")
    void invalid_xml_fails_schema_validation_and_returns_errorsOr() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/" + resourcePrefix() + "a.xsd")) {
            assertNotNull(in, "missing a.xsd in " + resourcePrefix());
            S schema = eng.loadSchema("a.xsd", in);

            ErrorsOr<Map<String, Object>> eo = eng.parseAndValidate("<a id=\"no-child\"/>", schema);

            assertTrue(eo.isError(), "XXE/schema violation should be rejected");
            assertFalse(eo.errorsOrThrow().isEmpty(), "Errors list should not be empty");
        }
    }

    @Test
    @DisplayName("DOCTYPE/XXE is disallowed (returns ErrorsOr(error))")
    void xxe_doctype_is_disallowed_and_returns_error() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/" + resourcePrefix() + "a.xsd")) {
            assertNotNull(in, "missing a.xsd in " + resourcePrefix());
            S schema = eng.loadSchema("a.xsd", in);

            String evil = """
                    <?xml version="1.0"?>
                    <!DOCTYPE a [ <!ENTITY xxe SYSTEM "file:///etc/passwd"> ]>
                    <a id="1"><x>&xxe;</x></a>
                    """;

            ErrorsOr<Map<String, Object>> eo = eng.parseAndValidate(evil, schema);

            assertTrue(eo.isError(), "XXE/DOCTYPE should be rejected");
            assertFalse(eo.errorsOrThrow().isEmpty());
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
        Map<String, Object> out = eng.parseAndValidate(xml, schema).valueOrThrow();
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
}
