package com.hcltech.rmg.xml;

import org.codehaus.stax2.validation.XMLValidationSchema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class WoodstoxXmlTypeClassTest extends AbstractXmlTypeClassTest<XMLValidationSchema> {
    public WoodstoxXmlTypeClassTest() {
        super(new WoodstoxXmlTypeClass());
    }

    @Test
    void engine_smoke_test() throws Exception {
        var eng = new WoodstoxXmlTypeClass();
        try (var in = getClass().getResourceAsStream("/testxmlloader/a.xsd")) {
            var schema = eng.loadSchema("a.xsd", in);
            var map = eng.parseAndValidate("<a id=\"1\"><x>x</x></a>", schema);
            assertNotNull(map.get("a"));
        }
    }
}
