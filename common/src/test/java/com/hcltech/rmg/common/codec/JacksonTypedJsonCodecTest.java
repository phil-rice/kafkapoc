// src/test/java/com/hcltech/rmg/common/codec/JacksonTypedJsonCodecMoreTest.java
package com.hcltech.rmg.common.codec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class JacksonTypedJsonCodecTest {

    record Person(String name, int age) {}

    /** Bean whose getter throws to force encode failure without subclassing ObjectMapper. */
    static class BadBean {
        public String getExplode() {
            throw new RuntimeException("boom-write");
        }
    }

    @Test
    void roundTrip_generic_via_TypeReference() {
        JacksonTypedJsonCodec<List<Person>> c =
                new JacksonTypedJsonCodec<>(new TypeReference<List<Person>>() {});

        List<Person> people = List.of(new Person("Alice", 42), new Person("Bob", 30));
        String json = c.encode(people).valueOrThrow();
        List<Person> back = c.decode(json).valueOrThrow();

        assertEquals(people, back);
    }

    @Test
    void roundTrip_generic_with_baseMapper_ctor() {
        ObjectMapper base = new ObjectMapper();
        JacksonTypedJsonCodec<List<Person>> c =
                new JacksonTypedJsonCodec<>(base, new TypeReference<List<Person>>() {});

        List<Person> people = List.of(new Person("Carol", 25));
        String json = c.encode(people).valueOrThrow();
        List<Person> back = c.decode(json).valueOrThrow();

        assertEquals(people, back);
    }

    @Test
    void objectMapper_returns_copy_not_same_instance() {
        ObjectMapper base = new ObjectMapper();
        JacksonTypedJsonCodec<Person> c = new JacksonTypedJsonCodec<>(base, Person.class);

        ObjectMapper fromCodec = c.objectMapper();
        assertNotSame(base, fromCodec, "Codec should copy the provided ObjectMapper");
        assertNotNull(fromCodec);
    }

    @Test
    void decode_with_bad_json_returns_error_class_path() {
        JacksonTypedJsonCodec<Person> c = new JacksonTypedJsonCodec<>(Person.class);

        var result = c.decode("{ this is not valid json");
        assertThrows(RuntimeException.class, result::valueOrThrow, "valueOrThrow should fail on bad JSON");
    }

    @Test
    void decode_with_type_mismatch_returns_error_class_path() {
        JacksonTypedJsonCodec<Person> c = new JacksonTypedJsonCodec<>(Person.class);

        // age is a string instead of an int
        var result = c.decode("{\"name\":\"Dave\",\"age\":\"old\"}");
        assertThrows(RuntimeException.class, result::valueOrThrow, "valueOrThrow should fail on type mismatch");
    }

    @Test
    void decode_with_type_mismatch_returns_error_typeref_path() {
        JacksonTypedJsonCodec<List<Person>> c =
                new JacksonTypedJsonCodec<>(new TypeReference<List<Person>>() {});

        // age wrong type inside array
        var result = c.decode("[{\"name\":\"Eve\",\"age\":\"nope\"}]");
        assertThrows(RuntimeException.class, result::valueOrThrow, "valueOrThrow should fail on type mismatch (TypeReference)");
    }

    @Test
    void encode_failure_is_wrapped_in_error() {
        JacksonTypedJsonCodec<BadBean> c = new JacksonTypedJsonCodec<>(BadBean.class);

        var result = c.encode(new BadBean());
        assertThrows(RuntimeException.class, result::valueOrThrow, "valueOrThrow should surface encode error");
    }

    @Test
    void constructors_validate_null_arguments() {
        // null base mapper (class)
        assertThrows(NullPointerException.class, () -> new JacksonTypedJsonCodec<>((ObjectMapper) null, Person.class));
        // null class
        assertThrows(NullPointerException.class, () -> new JacksonTypedJsonCodec<>((Class<Person>) null));

        // null base mapper (typeref)
        assertThrows(NullPointerException.class, () -> new JacksonTypedJsonCodec<>((ObjectMapper) null, new TypeReference<List<Person>>() {}));
        // null typeref
        assertThrows(NullPointerException.class, () -> new JacksonTypedJsonCodec<>((TypeReference<List<Person>>) null));
    }
}
