// src/test/java/com/hcltech/rmg/woodstox/WoodstoxXmlForMapStringObjectTypeClassTest.java
package com.hcltech.rmg.woodstox;

import com.ctc.wstx.exc.WstxLazyException;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.xml.AbstractXmlTypeClassTest;
import com.hcltech.rmg.xml.exceptions.XmlValidationException;
import org.codehaus.stax2.validation.XMLValidationSchema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WoodstoxXmlForMapStringObjectTypeClassTest
        extends AbstractXmlTypeClassTest<XMLValidationSchema> {

    WoodstoxXmlForMapStringObjectTypeClassTest() {
        super(new WoodstoxXmlForMapStringObjectTypeClass());
    }

    @Test
    @DisplayName("parseAndValidate: well-formedness error is wrapped (XMLStreamException or WstxLazyException)")
    void parse_wellformedness_error_is_wrapped() throws Exception {
        // minimal XSD: <a><x>...</x></a>
        String xsd = """
                <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
                  <xs:element name="a">
                    <xs:complexType>
                      <xs:sequence>
                        <xs:element name="x" type="xs:string"/>
                      </xs:sequence>
                    </xs:complexType>
                  </xs:element>
                </xs:schema>
                """;

        XMLValidationSchema schema;
        try (InputStream in = new ByteArrayInputStream(xsd.getBytes(StandardCharsets.UTF_8))) {
            schema = eng.loadSchema("a.xsd", in);
        }

        // Well-formedness error: unescaped '&'
        String malformed = "<a><x>bad & char</x></a>";

        XmlValidationException ex = assertThrows(XmlValidationException.class,
                () -> eng.parseAndValidate(malformed, schema));

        // Accept either catch block message
        String msg = ex.getMessage();
        assertTrue(
                msg.startsWith("XML parsing failed:") ||
                        msg.startsWith("Unexpected parsing error:"),
                "Message should be from XML parsing/Unexpected parsing wrapper, but was: " + msg
        );

        // And the cause should be an XMLStreamException (WstxLazyException extends it)
        assertNotNull(ex.getCause(), "Cause should be present");
        assertTrue(
                ex.getCause() instanceof WstxLazyException,
                "Cause should be WstxLazyException, but was: " + ex.getCause().getClass().getName()
        );
    }


    @Test
    @DisplayName("parseAndValidate: null schema triggers 'Unexpected parsing error: ...' branch")
    void parse_with_null_schema_hits_unexpected_parsing_error() {
        String xml = "<a id=\"1\"><x>ok</x></a>";
        XmlValidationException ex = assertThrows(XmlValidationException.class,
                () -> eng.parseAndValidate(xml, null));

        assertTrue(ex.getMessage().startsWith("Unexpected parsing error: "),
                "Null schema should lead to 'Unexpected parsing error: ...'");
    }

    @Test
    @DisplayName("extractId: nested text (<Id><part>A</part><part>BC</part></Id>) hits readElementText START_ELEMENT branch")
    void extractId_nested_text_coalesces_children() {
        String xml = """
                <Envelope>
                  <Body>
                    <Order>
                      <Id><part>A</part><part>BC</part></Id>
                    </Order>
                  </Body>
                </Envelope>
                """;

        ErrorsOr<String> id = eng.extractId(xml, List.of("Envelope", "Body", "Order", "Id"));
        assertTrue(id.isValue());
        assertEquals("ABC", id.valueOrThrow(), "Nested text should be coalesced and trimmed");
    }

    @Test
    @DisplayName("extractId: malformed XML hits XMLStreamException path and returns ErrorsOr.error(...)")
    void extractId_malformed_xml_reports_xmlstream_error() {
        // Malformed: missing closing tag
        String xml = "<root><id>123";
        var res = eng.extractId(xml, List.of("root", "id"));

        assertTrue(res.isError());
        String msg = res.errorsOrThrow().get(0);
        assertTrue(msg.startsWith("XML key extraction failed at path 'root/id': "),
                "Error should be wrapped with path and XMLStreamException message");
    }

    @Test
    @DisplayName("extractId: null xml and empty path produce specific validation errors")
    void extractId_null_and_empty_path_errors() {
        var e1 = eng.extractId(null, List.of("a"));
        assertTrue(e1.isError());
        assertTrue(e1.errorsOrThrow().get(0).contains("XML was null"));

        var e2 = eng.extractId("<a/>", List.of());
        assertTrue(e2.isError());
        assertTrue(e2.errorsOrThrow().get(0).contains("idPath cannot be empty"));
    }
}
