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
import java.util.Map;

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

    // ---------- New test: preserve singleton vs repeating tags ----------

    @Test
    @DisplayName("parseAndValidate: repeated siblings are preserved as List; single stays scalar")
    void parse_preserves_singleton_and_repeating_elements() throws Exception {
        // XSD: <test><output><out maxOccurs='unbounded'><count/></out></output></test>
        String xsd = """
                <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
                  <xs:element name="test">
                    <xs:complexType>
                      <xs:sequence>
                        <xs:element name="output">
                          <xs:complexType>
                            <xs:sequence>
                              <xs:element name="out" minOccurs="0" maxOccurs="unbounded">
                                <xs:complexType>
                                  <xs:sequence>
                                    <xs:element name="count" type="xs:long"/>
                                  </xs:sequence>
                                </xs:complexType>
                              </xs:element>
                            </xs:sequence>
                          </xs:complexType>
                        </xs:element>
                      </xs:sequence>
                    </xs:complexType>
                  </xs:element>
                </xs:schema>
                """;

        XMLValidationSchema schema;
        try (InputStream in = new ByteArrayInputStream(xsd.getBytes(StandardCharsets.UTF_8))) {
            schema = eng.loadSchema("test.xsd", in);
        }

        // Case 1: single <out>
        String xmlSingle = """
                <test>
                  <output>
                    <out><count>7</count></out>
                  </output>
                </test>
                """;

        Map<String, Object> parsedSingle = eng.parseAndValidate(xmlSingle, schema);

        // Navigate: test -> output -> out
        Map<String, Object> testMap1 = castMap(parsedSingle.get("test"));
        Map<String, Object> outputMap1 = castMap(testMap1.get("output"));
        Object out1 = outputMap1.get("out");

        assertNotNull(out1, "out should be present for single case");
        assertFalse(out1 instanceof List, "single <out> should remain scalar (not a List)");

        Map<String, Object> outObj1 = castMap(out1);
        assertEquals("7", outObj1.get("count"), "count inside single <out> should be '7'");

        // Case 2: repeated <out>
        String xmlRepeat = """
                <test>
                  <output>
                    <out><count>7</count></out>
                    <out><count>9</count></out>
                  </output>
                </test>
                """;

        Map<String, Object> parsedRepeat = eng.parseAndValidate(xmlRepeat, schema);

        Map<String, Object> testMap2 = castMap(parsedRepeat.get("test"));
        Map<String, Object> outputMap2 = castMap(testMap2.get("output"));
        Object out2 = outputMap2.get("out");

        assertNotNull(out2, "out should be present for repeat case");
        assertTrue(out2 instanceof List, "repeated <out> should be a List");

        @SuppressWarnings("unchecked")
        List<Object> outList = (List<Object>) out2;
        assertEquals(2, outList.size(), "there should be 2 <out> items");

        Map<String, Object> out0 = castMap(outList.get(0));
        Map<String, Object> out1Item = castMap(outList.get(1));
        assertEquals("7", out0.get("count"));
        assertEquals("9", out1Item.get("count"));
    }

    @Test
    @DisplayName("attributes: when present -> under 'attr'; absent -> no 'attr' key")
    void attributes_are_captured_only_when_present() throws Exception {
        String xsd = """
      <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
        <xs:element name="root">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="item" minOccurs="2" maxOccurs="2">
                <xs:complexType>
                  <xs:simpleContent>
                    <xs:extension base="xs:string">
                      <xs:attribute name="id" type="xs:string" use="optional"/>
                    </xs:extension>
                  </xs:simpleContent>
                </xs:complexType>
              </xs:element>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:schema>""";

        XMLValidationSchema schema;
        try (InputStream in = new ByteArrayInputStream(xsd.getBytes(StandardCharsets.UTF_8))) {
            schema = eng.loadSchema("attr.xsd", in);
        }

        String xml = """
      <root>
        <item id="A">alpha</item>
        <item>beta</item>
      </root>""";

        Map<String,Object> m = eng.parseAndValidate(xml, schema);
        Map<String,Object> root = castMap(m.get("root"));

        Object itemsNode = root.get("item");
        assertTrue(itemsNode instanceof List, "two <item> → List expected");
        @SuppressWarnings("unchecked")
        List<Object> items = (List<Object>) itemsNode;

        // First item has an attribute → non-leaf map with "attr" and "text"
        Map<String,Object> i0 = castMap(items.get(0));
        @SuppressWarnings("unchecked") Map<String,Object> attr0 = (Map<String,Object>) i0.get("attr");
        assertEquals("A", attr0.get("id"));
        assertEquals("alpha", i0.get("text"));

        // Second item has no attribute → pure leaf → String
        assertTrue(items.get(1) instanceof String, "attribute-less leaf stays String");
        assertEquals("beta", items.get(1));
    }

    @Test
    @DisplayName("mixed content: children + text -> 'text' key retained (trimmed); singleton child is String")
    void mixed_content_text_key_is_present() throws Exception {
        String xsd = """
      <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
        <xs:element name="p">
          <xs:complexType mixed="true">
            <xs:sequence>
              <xs:element name="b" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:schema>""";

        XMLValidationSchema schema;
        try (InputStream in = new ByteArrayInputStream(xsd.getBytes(StandardCharsets.UTF_8))) {
            schema = eng.loadSchema("mixed.xsd", in);
        }

        String xml = "<p>Hello <b>world</b> !</p>";

        Map<String,Object> m = eng.parseAndValidate(xml, schema);
        Map<String,Object> p = castMap(m.get("p"));

        // mixed text captured on "text"
        assertEquals("Hello  !", p.get("text"));

        // with one <b>, value is a String (not a List)
        assertTrue(p.get("b") instanceof String);
        assertEquals("world", p.get("b"));
    }

    @Test
    @DisplayName("CDATA and whitespace are coalesced/trimmed for leaves")
    void cdata_and_whitespace_trim() throws Exception {
        String xsd = """
      <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
        <xs:element name="r">
          <xs:complexType>
            <xs:sequence><xs:element name="x" type="xs:string"/></xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:schema>""";
        XMLValidationSchema schema;
        try (InputStream in = new ByteArrayInputStream(xsd.getBytes(StandardCharsets.UTF_8))) {
            schema = eng.loadSchema("cdata.xsd", in);
        }
        String xml = "<r><x>  <![CDATA[ hi ]]>  </x></r>";
        Map<String,Object> m = eng.parseAndValidate(xml, schema);
        Map<String,Object> r = castMap(m.get("r"));
        assertEquals("hi", r.get("x"));
    }
    @Test
    @DisplayName("repeated leaf elements -> List<String>; order preserved")
    void repeated_leaves_become_list_and_preserve_order() throws Exception {
        String xsd = """
      <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
        <xs:element name="r">
          <xs:complexType>
            <xs:sequence><xs:element name="tag" type="xs:string" minOccurs="0" maxOccurs="unbounded"/></xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:schema>""";
        XMLValidationSchema schema;
        try (InputStream in = new ByteArrayInputStream(xsd.getBytes(StandardCharsets.UTF_8))) {
            schema = eng.loadSchema("list.xsd", in);
        }
        String xml = "<r><tag>a</tag><tag>b</tag><tag>c</tag></r>";
        Map<String,Object> m = eng.parseAndValidate(xml, schema);
        Map<String,Object> r = castMap(m.get("r"));
        @SuppressWarnings("unchecked")
        List<String> tags = (List<String>) r.get("tag");
        assertEquals(List.of("a","b","c"), tags);
    }
    @Test
    @DisplayName("nested repeats: list under list works; singleton inner becomes String")
    void nested_repeats_supported() throws Exception {
        String xsd = """
      <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
        <xs:element name="root">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="group" minOccurs="0" maxOccurs="unbounded">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="item" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
                  </xs:sequence>
                </xs:complexType>
              </xs:element>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:schema>""";

        XMLValidationSchema schema;
        try (InputStream in = new ByteArrayInputStream(xsd.getBytes(StandardCharsets.UTF_8))) {
            schema = eng.loadSchema("nested.xsd", in);
        }

        String xml = "<root><group><item>a</item><item>b</item></group><group><item>c</item></group></root>";

        Map<String,Object> m = eng.parseAndValidate(xml, schema);
        Map<String,Object> root = castMap(m.get("root"));

        @SuppressWarnings("unchecked")
        List<Object> groups = (List<Object>) root.get("group");

        // group 0: two items → List<String>
        Object items0 = ((Map<?,?>) groups.get(0)).get("item");
        List<String> list0 = asStringList(items0);
        assertEquals(List.of("a","b"), list0);

        // group 1: one item → String (wrapped for assertion)
        Object items1 = ((Map<?,?>) groups.get(1)).get("item");
        List<String> list1 = asStringList(items1);
        assertEquals(List.of("c"), list1);
    }

    @SuppressWarnings("unchecked")
    private static List<String> asStringList(Object v) {
        if (v instanceof List<?> l) return (List<String>) l;
        return List.of((String) v); // singleton → wrap
    }

    @Test
    @DisplayName("empty/self-closing leaf becomes empty string")
    void empty_leaf_is_empty_string() throws Exception {
        String xsd = """
      <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
        <xs:element name="r">
          <xs:complexType>
            <xs:sequence><xs:element name="x" type="xs:string"/></xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:schema>""";
        XMLValidationSchema schema;
        try (InputStream in = new ByteArrayInputStream(xsd.getBytes(StandardCharsets.UTF_8))) {
            schema = eng.loadSchema("empty.xsd", in);
        }
        String xml = "<r><x/></r>";
        Map<String,Object> m = eng.parseAndValidate(xml, schema);
        Map<String,Object> r = castMap(m.get("r"));
        assertEquals("", r.get("x"));
    }
    @Test
    @DisplayName("schema validation errors propagate via ThrowingProblems (e.g., non-long in xs:long)")
    void schema_type_mismatch_throws_validation_exception() throws Exception {
        String xsd = """
      <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
        <xs:element name="r">
          <xs:complexType>
            <xs:sequence><xs:element name="count" type="xs:long"/></xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:schema>""";
        XMLValidationSchema schema;
        try (InputStream in = new ByteArrayInputStream(xsd.getBytes(StandardCharsets.UTF_8))) {
            schema = eng.loadSchema("val.xsd", in);
        }
        String xml = "<r><count>NaN</count></r>";
        assertThrows(XmlValidationException.class, () -> eng.parseAndValidate(xml, schema));
    }
    @Test
    @DisplayName("extractId path backtracks correctly with repeated nested names")
    void extractId_backtracks_over_similar_names() {
        String xml = """
      <a><b><a><b>OK</b></a></b></a>
      """;
        ErrorsOr<String> id = eng.extractId(xml, List.of("a","b","a","b"));
        assertTrue(id.isValue());
        assertEquals("OK", id.valueOrThrow());
    }


    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Object o) {
        assertNotNull(o, "Expected non-null map node");
        assertTrue(o instanceof Map, "Expected a Map but got " + o.getClass().getName());
        return (Map<String, Object>) o;
    }
}
