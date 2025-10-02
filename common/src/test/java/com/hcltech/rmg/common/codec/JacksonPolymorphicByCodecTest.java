// src/test/java/com/example/kafka/common/JacksonPolymorphicByCodecTest.java
package com.hcltech.rmg.common.codec;

import com.hcltech.rmg.common.testevent.AlphaEvent;
import com.hcltech.rmg.common.testevent.BetaEvent;
import com.hcltech.rmg.common.testevent.TestEvent;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JacksonPolymorphicByCodecTest {

    private Codec<TestEvent, String> poly() {
        // subtype codecs (typed)
        Codec<AlphaEvent, String> alpha = Codec.clazzCodec(AlphaEvent.class);
        Codec<BetaEvent, String> beta = Codec.clazzCodec(BetaEvent.class);

        // discriminator resolver for encode
        java.util.function.Function<TestEvent, String> disc =
                e -> (e instanceof AlphaEvent) ? "alpha"
                        : (e instanceof BetaEvent) ? "beta"
                        : null;

        // compose
        return Codec.polymorphicCodec(
                disc,
                Map.of(
                        "alpha", (Codec<? extends TestEvent, String>) alpha,
                        "beta", (Codec<? extends TestEvent, String>) beta
                )
        );
    }

    @Test
    void roundTrip_alpha_event() throws Exception {
        Codec<TestEvent, String> c = poly();

        TestEvent in = new AlphaEvent("$.name", "Alice");
        String json = c.encode(in).valueOrThrow();

        // wrapper structure
        assertTrue(json.contains("\"type\":\"alpha\""));
        assertTrue(json.contains("\"payload\""));
        assertFalse(json.contains("\\\"payload\\\"")); // not double-encoded

        TestEvent out = c.decode(json).valueOrThrow();
        assertInstanceOf(AlphaEvent.class, out);

        AlphaEvent ae = (AlphaEvent) out;
        assertEquals("$.name", ae.path());
        assertEquals("Alice", ae.value());
    }

    @Test
    void roundTrip_beta_event_bytes_wrapper() throws Exception {
        Codec<TestEvent, byte[]> bytes = Codec.bytes(poly());

        TestEvent in = new BetaEvent("$.scores", 99);
        byte[] enc = bytes.encode(in).valueOrThrow();

        String s = new String(enc, StandardCharsets.UTF_8);
        assertTrue(s.contains("\"type\":\"beta\""));
        assertTrue(s.contains("\"payload\""));

        TestEvent out = bytes.decode(enc).valueOrThrow();
        assertInstanceOf(BetaEvent.class, out);

        BetaEvent be = (BetaEvent) out;
        assertEquals("$.scores", be.path());
        assertEquals(99, be.value());
    }

    @Test
    void decode_unknown_discriminator_fails_nicely() {
        Codec<TestEvent, String> c = poly();
        String bad = "{\"type\":\"gamma\",\"payload\":{}}";
        var ex = c.decode(bad).errorsOrThrow();
        assertEquals(List.of("Failed to decode polymorphic type: IllegalArgumentException: Unknown discriminator: gamma"), ex);
    }
}
