// src/test/java/com/example/kafka/common/LineSeparatedListCodecTest.java
package com.hcltech.rmg.common;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LineSeparatedListCodecTest {

    record Person(String name, int age) {}

    @Test
    void roundTrip_people() throws Exception {
        Codec<Person, String> personJson = Codec.clazzCodec(Person.class);
        Codec<List<Person>, String> lines = new LineSeparatedListCodec<>(personJson);

        List<Person> input = List.of(new Person("Alice", 40), new Person("Bob", 31));
        String wire = lines.encode(input);

        assertTrue(wire.contains("\n"));
        assertTrue(wire.contains("\"name\":\"Alice\""));
        assertTrue(wire.contains("\"name\":\"Bob\""));

        List<Person> back = lines.decode(wire);
        assertEquals(input, back);
    }

    @Test
    void empty_list_is_empty_string_and_back() throws Exception {
        Codec<Integer, String> intJson = Codec.clazzCodec(Integer.class);
        Codec<List<Integer>, String> lines = new LineSeparatedListCodec<>(intJson);

        String wire = lines.encode(List.of());
        assertEquals("", wire);

        List<Integer> back = lines.decode(wire);
        assertTrue(back.isEmpty());
    }

    @Test
    void decode_tolerates_trailing_newline() throws Exception {
        Codec<Integer, String> intJson = Codec.clazzCodec(Integer.class);
        Codec<List<Integer>, String> lines = new LineSeparatedListCodec<>(intJson);

        List<Integer> back = lines.decode("1\n2\n3\n");
        assertEquals(List.of(1, 2, 3), back);
    }

    @Test
    void interior_empty_line_is_empty_element_if_item_codec_supports_it() throws Exception {
        // With String JSON, empty string is "\"\""
        Codec<String, String> stringJson = Codec.clazzCodec(String.class);
        Codec<List<String>, String> lines = new LineSeparatedListCodec<>(stringJson);

        List<String> input = List.of("", "x", "");
        String wire = lines.encode(input);
        assertEquals("\"\"\n\"x\"\n\"\"", wire);

        List<String> back = lines.decode(wire);
        assertEquals(input, back);
    }

    @Test
    void helper_builds_codec() throws Exception {
        Codec<Integer, String> intJson = Codec.clazzCodec(Integer.class);
        Codec<List<Integer>, String> lines = Codec.lines(intJson);

        String wire = lines.encode(List.of(10, 20));
        assertEquals("10\n20", wire);
        assertEquals(List.of(10, 20), lines.decode(wire));
    }
}
