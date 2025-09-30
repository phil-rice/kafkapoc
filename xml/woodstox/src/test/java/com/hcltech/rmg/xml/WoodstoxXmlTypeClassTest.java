// src/test/java/com/hcltech/rmg/xml/WoodstoxXmlTypeClassTest.java
package com.hcltech.rmg.xml;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import org.codehaus.stax2.validation.XMLValidationSchema;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
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

            // Keep this assertion from your original test
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
            // Message is produced via ErrorsOr.error("Parsing xml {0}: {1}", e)
            // so it should start with the exception class name
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

        var in = new java.io.ByteArrayInputStream(badXsd.getBytes(java.nio.charset.StandardCharsets.UTF_8));
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
            // We don't rely on exact wording across Woodstox versions—just sanity-check:
            assertTrue(msg.startsWith("Parsing xml"), "Expected formatted error message: " + msg);
        }
    }
}
