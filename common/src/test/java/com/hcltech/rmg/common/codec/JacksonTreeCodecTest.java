package com.hcltech.rmg.common.codec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JacksonTreeCodecTest {

    // Simple domain record for testing
    public static class Person {
        public final String name;
        public final int age;

        @JsonCreator
        public Person(@JsonProperty("name") String name,
                      @JsonProperty("age") int age) {
            this.name = name;
            this.age = age;
        }

        // equals/hashCode for assertions
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Person)) return false;
            Person other = (Person) o;
            return age == other.age && name.equals(other.name);
        }

        @Override
        public int hashCode() {
            return name.hashCode() * 31 + age;
        }
    }

    @Test
    void encodeProducesMapWithFields() {
        JacksonTreeCodec<Person> codec = new JacksonTreeCodec<>(Person.class);
        Person p = new Person("Alice", 30);

        Map<String, Object> encoded = codec.encode(p).valueOrThrow();

        assertEquals("Alice", encoded.get("name"));
        assertEquals(30, encoded.get("age"));
    }

    @Test
    void decodeReconstructsObject() {
        JacksonTreeCodec<Person> codec = new JacksonTreeCodec<>(Person.class);
        Map<String, Object> map = Map.of("name", "Bob", "age", 40);

        Person decoded = codec.decode(map).valueOrThrow();

        assertEquals(new Person("Bob", 40), decoded);
    }

    @Test
    void roundTripMaintainsEquality() {
        JacksonTreeCodec<Person> codec = new JacksonTreeCodec<>(Person.class);
        Person original = new Person("Carol", 25);

        Map<String, Object> encoded = codec.encode(original).valueOrThrow();
        Person decoded = codec.decode(encoded).valueOrThrow();

        assertEquals(original, decoded);
    }

    @Test
    void decodeIgnoresExtraFields() {
        JacksonTreeCodec<Person> codec = new JacksonTreeCodec<>(Person.class);
        Map<String, Object> map = Map.of(
                "name", "Dan",
                "age", 50,
                "extra", "ignored"
        );

        Person decoded = codec.decode(map).valueOrThrow();

        assertEquals(new Person("Dan", 50), decoded);
    }

    @Test
    void encodeHandlesNullValues() {
        JacksonTreeCodec<Person> codec = new JacksonTreeCodec<>(Person.class);
        Person p = new Person(null, 0);

        Map<String, Object> encoded = codec.encode(p).valueOrThrow();

        assertTrue(encoded.containsKey("name"));
        assertNull(encoded.get("name"));
        assertEquals(0, encoded.get("age"));
    }
}
