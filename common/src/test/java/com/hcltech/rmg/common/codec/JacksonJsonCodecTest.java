// src/test/java/com/example/kafka/common/JacksonJsonCodecTest.java
package com.hcltech.rmg.common.codec;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JacksonJsonCodecTest {
    @Test
    void roundTrip_map_list_primitives() throws Exception {
        Codec<Object, String> c = new JacksonJsonCodec();

        Map<String, Object> payload = new java.util.LinkedHashMap<>();
        payload.put("n", 123);
        payload.put("s", "abc");
        payload.put("b", true);
        payload.put("list", java.util.List.of(1, 2, 3));
        payload.put("null", null); // allowed now

        String json = c.encode(payload).valueOrThrow();
        assertNotNull(json);
        assertTrue(json.contains("\"n\":123"));

        Object decoded = c.decode(json).valueOrThrow();
        assertInstanceOf(Map.class, decoded);
        Map<?, ?> m = (Map<?, ?>) decoded;

        assertEquals(123, m.get("n"));
        assertEquals("abc", m.get("s"));
        assertEquals(true, m.get("b"));
        assertEquals(java.util.List.of(1, 2, 3), m.get("list"));

        // For nulls: ensure the key is present and value is null
        assertTrue(m.containsKey("null"));
        assertNull(m.get("null"));
    }


    @Test
    void helper_method_codec_json_returns_JacksonJsonCodec() {
        Codec<Object, String> helper = Codec.json();
        assertNotNull(helper);
        assertEquals(JacksonJsonCodec.class, helper.getClass());
    }
}
