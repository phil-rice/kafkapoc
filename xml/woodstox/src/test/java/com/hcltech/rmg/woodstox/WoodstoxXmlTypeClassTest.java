// src/test/java/com/hcltech/rmg/xml/WoodstoxXmlTypeClassTest.java
package com.hcltech.rmg.woodstox;

import com.hcltech.rmg.xml.exceptions.LoadSchemaException;
import org.codehaus.stax2.validation.XMLValidationSchema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import com.hcltech.rmg.xml.AbstractXmlTypeClassTest;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Woodstox-specific additions to the engine-agnostic tests in AbstractXmlTypeClassTest.
 */
public class WoodstoxXmlTypeClassTest extends AbstractXmlTypeClassTest<XMLValidationSchema> {

    public WoodstoxXmlTypeClassTest() {
        super(new WoodstoxXmlTypeClass());
    }

    @Test
    @DisplayName("malformed schema → LoadSchemaException (Woodstox-specific contract)")
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
                LoadSchemaException.class,
                () -> eng.loadSchema("malformed.xsd", in),
                "Expected LoadSchemaException for malformed schema"
        );
    }
}
