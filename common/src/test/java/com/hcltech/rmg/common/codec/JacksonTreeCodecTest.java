// src/test/java/com/hcltech/rmg/common/codec/JacksonTreeCodecTest.java
package com.hcltech.rmg.common.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JacksonTreeCodecTest {

    record Person(String name, int age) {}

    /** Bean whose getter throws to force encode failure. */
    static class BadBean {
        public String getExplode() {
            throw new RuntimeException("boom");
        }
    }

    @Test
    void roundTrip_person_to_map_and_back() {
        JacksonTreeCodec<Person> c = new JacksonTreeCodec<>(Person.class);

        Person alice = new Person("Alice", 42);
        Map<String, Object> asMap = c.encode(alice).valueOrThrow();

        assertEquals("Alice", asMap.get("name"));
        assertEquals(42, asMap.get("age"));

        Person back = c.decode(asMap).valueOrThrow();
        assertEquals(alice, back);
    }

    @Test
    void objectMapper_is_a_copy_not_the_same_instance() {
        ObjectMapper base = new ObjectMapper();
        JacksonTreeCodec<Person> c = new JacksonTreeCodec<>(base, Person.class);

        ObjectMapper fromCodec = c.objectMapper();
        assertNotSame(base, fromCodec, "Codec should copy the provided ObjectMapper");
        assertNotNull(fromCodec);
    }

    @Test
    void decode_ignores_unknown_properties() {
        JacksonTreeCodec<Person> c = new JacksonTreeCodec<>(Person.class);

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("name", "Bob");
        map.put("age", 30);
        map.put("extra", "ignored"); // should be ignored due to FAIL_ON_UNKNOWN_PROPERTIES=false

        Person back = c.decode(map).valueOrThrow();
        assertEquals(new Person("Bob", 30), back);
    }

    @Test
    void decode_type_mismatch_returns_error() {
        JacksonTreeCodec<Person> c = new JacksonTreeCodec<>(Person.class);

        Map<String, Object> bad = Map.of("name", "Carol", "age", "notAnInt");

        var res = c.decode(bad);
        assertTrue(res.isError());
        assertThrows(IllegalStateException.class, res::valueOrThrow);
        assertFalse(res.errorsOrThrow().isEmpty());
    }

    @Test
    void encode_failure_is_wrapped_in_error() {
        JacksonTreeCodec<BadBean> c = new JacksonTreeCodec<>(BadBean.class);

        var res = c.encode(new BadBean());
        assertTrue(res.isError());
        assertThrows(IllegalStateException.class, res::valueOrThrow);
        assertFalse(res.errorsOrThrow().isEmpty());
    }
}
