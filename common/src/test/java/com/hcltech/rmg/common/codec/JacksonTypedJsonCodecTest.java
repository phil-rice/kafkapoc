// src/test/java/com/example/kafka/common/JacksonTypedJsonCodecTest.java
package com.hcltech.rmg.common.codec;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JacksonTypedJsonCodecTest {

    record Person(String name, int age) {}

    @Test
    void roundTrip_typed_record() throws Exception {
        Codec<Person, String> c = new JacksonTypedJsonCodec<>(Person.class);

        Person alice = new Person("Alice", 42);
        String json = c.encode(alice);

        assertTrue(json.contains("\"name\":\"Alice\""));
        assertTrue(json.contains("\"age\":42"));

        Person back = c.decode(json);
        assertEquals(alice, back);
    }

    @Test
    void helper_method_codec_json_typed() throws Exception {
        Codec<Person, String> c = Codec.clazzCodec(Person.class);
        String json = c.encode(new Person("Bob", 30));
        Person back = c.decode(json);

        assertEquals(new Person("Bob", 30), back);
        assertEquals(JacksonTypedJsonCodec.class, c.getClass());
    }
}
