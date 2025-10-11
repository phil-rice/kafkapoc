// src/test/java/com/hcltech/rmg/common/codec/CodecFactoriesAndInvertTest.java
package com.hcltech.rmg.common.codec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class CodecTest {

    // Simple test type(s)
    record Person(String name, int age) {}
    sealed interface Animal permits Dog, Cat {}
    record Dog(String name) implements Animal {}
    record Cat(String name) implements Animal {}

    @Test
    void invert_roundtrip_on_bytes_wrapper() {
        // Person -> String via JSON, then wrap as bytes
        Codec<Person, String> asJson = Codec.clazzCodec(Person.class);
        Codec<Person, byte[]> asBytes = Codec.bytes(asJson);

        // Exercise invert(): Codec<byte[], Person>
        Codec<byte[], Person> inverted = asBytes.invert();

        Person alice = new Person("Alice", 42);
        byte[] encoded = asBytes.encode(alice).valueOrThrow();

        // encode(byte[]) on inverted calls original decode(String...) through the invert impl
        Person decodedViaInverted = inverted.encode(encoded).valueOrThrow();
        assertEquals(alice, decodedViaInverted);

        // decode(Person) on inverted calls original encode(...) (to bytes)
        byte[] backBytes = inverted.decode(alice).valueOrThrow();
        assertArrayEquals(encoded, backBytes);
    }

    @Test
    void lines_factory_roundtrip() {
        // item codec: integers as JSON strings (so we reuse stable path)
        Codec<Integer, String> intJson = Codec.clazzCodec(Integer.class);
        Codec<List<Integer>, String> lines = Codec.lines(intJson);

        List<Integer> nums = List.of(1, 2, 3);
        String text = lines.encode(nums).valueOrThrow();

        // Expect line-separated payload
        String[] split = text.split("\\R");
        assertEquals(3, split.length);

        List<Integer> back = lines.decode(text).valueOrThrow();
        assertEquals(nums, back);
    }

    @Test
    void jsonTree_roundtrip_and_copy_back_to_object() {
        Codec<Person, Map<String, Object>> tree = Codec.jsonTree(Person.class);

        Person p = new Person("Bob", 30);
        Map<String, Object> map = tree.encode(p).valueOrThrow();
        assertEquals("Bob", map.get("name"));
        assertEquals(30, map.get("age"));

        Person back = tree.decode(map).valueOrThrow();
        assertEquals(p, back);
    }

    @Test
    void jsonTree_error_on_bad_map() {
        Codec<Person, Map<String, Object>> tree = Codec.jsonTree(Person.class);

        // Wrong shape: age is a string instead of a number
        Map<String, Object> bad = Map.of("name", "Oops", "age", "notAnInt");

        var result = tree.decode(bad);
        assertThrows(IllegalStateException.class, result::valueOrThrow);
        var errs = result.errorsOrThrow();
        assertFalse(errs.isEmpty());
    }

    @Test
    void json_factory_roundtrip_with_mixed_map() {
        Codec<Object, String> json = Codec.json();

        Map<String, Object> payload = Map.of(
                "s", "str",
                "n", 123,
                "b", true,
                "arr", List.of(1, 2, 3)
        );
        String s = json.encode(payload).valueOrThrow();

        // Decode to a raw Map via the same generic json() (it should yield a Map-like structure)
        Object back = json.decode(s).valueOrThrow();
        assertInstanceOf(Map.class, back);
        Map<?, ?> m = (Map<?, ?>) back;
        assertEquals(123, m.get("n"));
        assertEquals(true, m.get("b"));
    }

    @Test
    void clazzCodec_roundtrip_person() {
        Codec<Person, String> c = Codec.clazzCodec(Person.class);
        String s = c.encode(new Person("Carol", 25)).valueOrThrow();
        Person back = c.decode(s).valueOrThrow();
        assertEquals(new Person("Carol", 25), back);
    }

    @Test
    void polymorphicCodec_roundtrip_and_missing_discriminator_error() {
        // Build subtype map
        Codec<Dog, String> dogCodec = Codec.clazzCodec(Dog.class);
        Codec<Cat, String> catCodec = Codec.clazzCodec(Cat.class);

        Map<String, Codec<? extends Animal, String>> subtypes = Map.of(
                "dog", dogCodec,
                "cat", catCodec
        );

        Codec<Animal, String> poly =
                Codec.polymorphicCodec(a -> a instanceof Dog ? "dog" : "cat", subtypes);

        // Happy path: encode/decode a Dog
        Dog d = new Dog("Rex");
        String dogJson = poly.encode(d).valueOrThrow();
        Animal dogBack = poly.decode(dogJson).valueOrThrow();
        assertEquals(d, dogBack);

        // Error path: discriminator not in the map
        Codec<Animal, String> brokenPoly =
                Codec.polymorphicCodec(a -> "bird", subtypes); // "bird" isn't mapped

        var encResult = brokenPoly.encode(new Cat("Mog"));
        assertThrows(IllegalStateException.class, encResult::valueOrThrow);
        var errs = encResult.errorsOrThrow();
        assertFalse(errs.isEmpty());
    }

    @Test
    void bytes_wrapper_roundtrip() {
        Codec<Person, String> personJson = Codec.clazzCodec(Person.class);
        Codec<Person, byte[]> bytes = Codec.bytes(personJson);

        Person p = new Person("Dave", 33);
        byte[] bs = bytes.encode(p).valueOrThrow();
        assertNotEquals(0, bs.length);
        // sanity: looks like UTF-8 JSON
        String asString = new String(bs, StandardCharsets.UTF_8);
        assertTrue(asString.contains("\"name\":\"Dave\""));

        Person back = bytes.decode(bs).valueOrThrow();
        assertEquals(p, back);
    }

    @Test
    void typeRef_path_is_reachable_via_user_codec_and_lines() {
        // Exercise TypeReference path indirectly (helps overall coverage for typed codec generics)
        Codec<List<Person>, String> listAsJson =
                new JacksonTypedJsonCodec<>(new TypeReference<List<Person>>() {});
        List<Person> ps = List.of(new Person("Eve", 28));
        String json = listAsJson.encode(ps).valueOrThrow();
        List<Person> back = listAsJson.decode(json).valueOrThrow();
        assertEquals(ps, back);
    }

    @Test
    void errorsor_helpers_value_and_errors_branches_are_usable_with_codecs() {
        Codec<Person, String> c = Codec.clazzCodec(Person.class);

        // value branch
        ErrorsOr<String> ok = c.encode(new Person("Frank", 40));
        assertTrue(ok.isValue());
        assertEquals("Frank", c.decode(ok.valueOrThrow()).valueOrThrow().name());

        // error branch (bad JSON)
        ErrorsOr<Person> bad = c.decode("{ not json");
        assertTrue(bad.isError());
        var errs = bad.errorsOrThrow();
        assertFalse(errs.isEmpty());

        // recover() use-site sanity with codecs
        Person recovered = bad.recover(es -> new Person("default", 0)).valueOrThrow();
        assertEquals("default", recovered.name());
    }
}
