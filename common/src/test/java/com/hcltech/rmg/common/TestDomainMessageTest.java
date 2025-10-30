package com.hcltech.rmg.common;

import com.hcltech.rmg.common.codec.Codec;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TestDomainMessageTest {

    private static final Codec<TestDomainMessage, String> XML = TestDomainMessage.xmlCodec;

    @Test
    void xml_roundtrip_happy_path() {
        TestDomainMessage in = new TestDomainMessage("153", "evt", "prod", "acme", 7L);

        String xml = XML.encode(in).valueOrThrow();
        assertTrue(xml.startsWith("<msg>") && xml.endsWith("</msg>"));
        assertTrue(xml.contains("<domainId>153</domainId>"));
        assertTrue(xml.contains("<eventType>evt</eventType>"));
        assertTrue(xml.contains("<productType>prod</productType>"));
        assertTrue(xml.contains("<company>acme</company>"));
        assertTrue(xml.contains("<count>7</count>"));

        TestDomainMessage out = XML.decode(xml).valueOrThrow();
        assertEquals(in, out);
    }

    @Test
    void decode_trims_whitespace_in_count() {
        String xml = """
            <msg>
              <domainId>A</domainId>
              <eventType>E</eventType>
              <productType>P</productType>
              <company>C</company>
              <count>   42   </count>
            </msg>
            """;
        TestDomainMessage out = XML.decode(xml).valueOrThrow();
        assertEquals(42L, out.count());
    }

    @Test
    void decode_errors_when_required_tag_missing_entirely() {
        // drop <company>…</company> entirely
        String xml = """
            <msg>
              <domainId>A</domainId>
              <eventType>E</eventType>
              <productType>P</productType>
              <count>1</count>
            </msg>
            """;
        ErrorsOr<TestDomainMessage> res = XML.decode(xml);
        assertTrue(res.isError());
        assertTrue(res.errorsOrThrow().get(0).contains("missing required fields"));
    }

    @Test
    void decode_errors_when_count_is_not_a_number() {
        String xml = """
            <msg>
              <domainId>A</domainId>
              <eventType>E</eventType>
              <productType>P</productType>
              <company>C</company>
              <count>NaN</count>
            </msg>
            """;
        ErrorsOr<TestDomainMessage> res = XML.decode(xml);
        assertTrue(res.isError());
        // message is "Failed to decode XML: " + e.getMessage()
        assertTrue(res.errorsOrThrow().get(0).startsWith("Failed to decode XML:"));
    }

    @Test
    void encode_allows_null_string_fields_resulting_in_empty_elements_and_roundtrips_as_empty_strings() {
        TestDomainMessage in = new TestDomainMessage(null, null, null, null, 0L);

        String xml = XML.encode(in).valueOrThrow();
        // tags present with empty content
        assertTrue(xml.contains("<domainId></domainId>"));
        assertTrue(xml.contains("<eventType></eventType>"));
        assertTrue(xml.contains("<productType></productType>"));
        assertTrue(xml.contains("<company></company>"));
        assertTrue(xml.contains("<count>0</count>"));

        // decode treats empty content as empty strings, not nulls
        TestDomainMessage out = XML.decode(xml).valueOrThrow();
        assertEquals("", out.domainId());
        assertEquals("", out.eventType());
        assertEquals("", out.productType());
        assertEquals("", out.company());
        assertEquals(0L, out.count());
    }

    @Test
    void decode_errors_when_closing_tag_missing() {
        // missing </eventType> -> getTag returns null -> "missing required fields"
        String xml = """
            <msg>
              <domainId>A</domainId>
              <eventType>open
              <productType>P</productType>
              <company>C</company>
              <count>1</count>
            </msg>
            """;
        ErrorsOr<TestDomainMessage> res = XML.decode(xml);
        assertTrue(res.isError());
        assertTrue(res.errorsOrThrow().get(0).contains("missing required fields"));
    }

    @Test
    void convenience_create_defaults_and_overload() {
        TestDomainMessage a = TestDomainMessage.create("D-1", null);
        assertEquals("D-1", a.domainId());
        assertEquals("test-event", a.eventType());
        assertEquals("parcel", a.productType());
        assertEquals("rmg", a.company());
        assertEquals(0L, a.count());

        TestDomainMessage b = TestDomainMessage.create("D-2", "etype", "ptype", "co", 9L);
        assertEquals(new TestDomainMessage("D-2", "etype", "ptype", "co", 9L), b);
    }
}
