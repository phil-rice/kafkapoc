// src/test/java/com/hcltech/rmg/xml/WoodstoxXmlTypeClassTest.java
package com.hcltech.rmg.xml;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import org.codehaus.stax2.validation.XMLValidationSchema;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class WoodstoxXmlTypeClassTest extends AbstractXmlTypeClassTest<XMLValidationSchema> {

    public WoodstoxXmlTypeClassTest() {
        super(new WoodstoxXmlTypeClass());
    }

    @Test
    void engine_smoke_test_value_roundtrips() throws Exception {
        var eng = new WoodstoxXmlTypeClass();
        try (InputStream in = getClass().getResourceAsStream("/testxmlloader/a.xsd")) {
            var schema = eng.loadSchema("a.xsd", in);

            ErrorsOr<Map<String, Object>> eo = eng.parseAndValidate("<a id=\"1\"><x>x</x></a>", schema);
            Map<String, Object> map = eo.valueOrThrow(); // unwrap value

            assertNotNull(map.get("a"));
        }
    }

    @Test
    void invalid_xml_fails_schema_validation_and_returns_errorsOr() throws Exception {
        var eng = new WoodstoxXmlTypeClass();
        try (InputStream in = getClass().getResourceAsStream("/testxmlloader/a.xsd")) {
            var schema = eng.loadSchema("a.xsd", in);

            // Missing required child <x> (assuming schema requires it)
            ErrorsOr<Map<String, Object>> eo = eng.parseAndValidate("<a id=\"no-child\"/>", schema);

            assertTrue(eo.isError(), "Expected ErrorsOr in error state for schema violation");
            List<String> errs = eo.errorsOrThrow(); // unwrap errors

            assertFalse(errs.isEmpty(), "Errors list should not be empty");
            assertTrue(errs.get(0).startsWith("Parsing xml XmlValidationException:"),
                    "Should contain XmlValidationException: " + errs.get(0));
        }
    }

    @Test
    void malformed_schema_returns_LoadSchemaException() {
        var eng = new WoodstoxXmlTypeClass();

        // Intentionally broken XSD (bad closing tag + undefined type)
        String badXsd = """
                <?xml version="1.0" encoding="UTF-8"?>
                <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
                  <xs:element name="brokenElement">
                    <xs:complexType>
                      <xs:sequence>
                        <xs:element name="child" type="xs:nonExistingType"/>
                      </xs:sequence>
                    </xs:complexType>
                  </xs:element>
                </xs:schem>  <!-- wrong closing tag -->
                """;
        var in = new java.io.ByteArrayInputStream(badXsd.getBytes(StandardCharsets.UTF_8));

        assertThrows(
                com.hcltech.rmg.xml.exceptions.LoadSchemaException.class,
                () -> eng.loadSchema("malformed.xsd", in),
                "Expected LoadSchemaException for malformed schema"
        );
    }

    @Test
    void xxe_doctype_is_disallowed_and_returns_error() throws Exception {
        var eng = new WoodstoxXmlTypeClass();
        try (InputStream in = getClass().getResourceAsStream("/testxmlloader/a.xsd")) {
            var schema = eng.loadSchema("a.xsd", in);

            // DOCTYPE is disabled via factory properties; parsing should error
            String evil = """
                    <?xml version="1.0"?>
                    <!DOCTYPE a [ <!ENTITY xxe SYSTEM "file:///etc/passwd"> ]>
                    <a id="1"><x>&xxe;</x></a>
                    """;

            ErrorsOr<Map<String, Object>> eo = eng.parseAndValidate(evil, schema);

            assertTrue(eo.isError(), "XXE/DOCTYPE should be rejected");
            String msg = eo.errorsOrThrow().get(0);
            assertTrue(msg.startsWith("Parsing xml"), "Expected formatted error message: " + msg);
        }
    }

    // -----------------------------
    // NEW: extractId(...) test cases
    // -----------------------------

    @Test
    void extractId_happy_path_with_simple_path() {
        var eng = new WoodstoxXmlTypeClass();

        String xml = """
                <Envelope>
                  <Body>
                    <Order>
                      <Id>ABC-123</Id>
                    </Order>
                  </Body>
                </Envelope>
                """;

        ErrorsOr<String> id = eng.extractId(xml, "Envelope/Body/Order/Id");
        assertTrue(id.isValue(), "Expected id to be extracted");
        assertEquals("ABC-123", id.valueOrThrow());
    }

    @Test
    void extractId_returns_error_when_not_found() {
        var eng = new WoodstoxXmlTypeClass();

        String xml = "<root><child>v</child></root>";
        ErrorsOr<String> id = eng.extractId(xml, "root/missing");

        assertTrue(id.isError(), "Expected error when id path not found");
        String msg = id.errorsOrThrow().get(0);
        assertTrue(msg.contains("Key not found"), "Message should indicate missing key path: " + msg);
    }

    @Test
    void extractId_returns_error_when_empty_text() {
        var eng = new WoodstoxXmlTypeClass();

        String xml = "<root><id>   </id></root>";
        ErrorsOr<String> id = eng.extractId(xml, "root/id");

        assertTrue(id.isError(), "Expected error for empty id element");
    }
}
