package com.hcltech.rmg.common.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.common.testevent.AlphaEvent;
import com.hcltech.rmg.common.testevent.BetaEvent;
import com.hcltech.rmg.common.testevent.TestEvent;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

public class JacksonPolymorphicByCodecTest {

    /** Convenience: real typed codecs for happy wiring. */
    private static final Codec<AlphaEvent, String> ALPHA_JSON = Codec.clazzCodec(AlphaEvent.class);
    private static final Codec<BetaEvent, String>  BETA_JSON  = Codec.clazzCodec(BetaEvent.class);

    /** Discriminator used in some tests. */
    private static final Function<TestEvent, String> DISC =
            e -> (e instanceof AlphaEvent) ? "alpha"
                    : (e instanceof BetaEvent) ? "beta"
                    : null;

    @Test
    void encode_error_when_typeOf_returns_null() {
        // discriminator always returns null -> NPE inside encode
        Codec<TestEvent, String> poly =
                new JacksonPolymorphicByCodec<>(e -> null, Map.of("alpha", ALPHA_JSON, "beta", BETA_JSON));

        ErrorsOr<String> res = poly.encode(new AlphaEvent("$.p", "x"));
        assertTrue(res.isError());
        var errs = res.errorsOrThrow();
        assertEquals(1, errs.size());
        assertTrue(errs.get(0).contains("Failed to encode polymorphic type: NullPointerException: typeOf returned null"));
    }

    @Test
    void encode_error_when_discriminator_not_registered() {
        // returns a discriminator that is not in the map
        Codec<TestEvent, String> poly =
                new JacksonPolymorphicByCodec<>(e -> "gamma", Map.of("alpha", ALPHA_JSON, "beta", BETA_JSON));

        ErrorsOr<String> res = poly.encode(new AlphaEvent("$.p", "x"));
        assertTrue(res.isError());
        assertTrue(res.errorsOrThrow().get(0)
                .contains("Failed to encode polymorphic type: IllegalArgumentException: No codec registered for discriminator: gamma"));
    }

    @Test
    void encode_propagates_sub_encoder_error_via_errorCast() {
        // Subtype codec that fails on encode (and should pass error through, NOT wrap)
        Codec<AlphaEvent, String> failingAlpha = new Codec<>() {
            @Override public ErrorsOr<String> encode(AlphaEvent from) { return ErrorsOr.error("sub-encode-error"); }
            @Override public ErrorsOr<AlphaEvent> decode(String to)    { return ErrorsOr.error("not-used"); }
        };

        Codec<TestEvent, String> poly =
                new JacksonPolymorphicByCodec<>(DISC, Map.of("alpha", failingAlpha, "beta", BETA_JSON));

        var res = poly.encode(new AlphaEvent("$.p", "x"));
        assertTrue(res.isError());
        assertEquals("sub-encode-error", res.errorsOrThrow().get(0)); // direct passthrough
    }

    @Test
    void decode_error_when_missing_type_field() {
        Codec<TestEvent, String> poly =
                new JacksonPolymorphicByCodec<>(DISC, Map.of("alpha", ALPHA_JSON, "beta", BETA_JSON));

        var res = poly.decode("{\"payload\":{}}");
        assertTrue(res.isError());
        assertTrue(res.errorsOrThrow().get(0)
                .contains("Failed to decode polymorphic type: IllegalArgumentException: Missing textual 'type' field"));
    }

    @Test
    void decode_error_when_type_not_textual() {
        Codec<TestEvent, String> poly =
                new JacksonPolymorphicByCodec<>(DISC, Map.of("alpha", ALPHA_JSON, "beta", BETA_JSON));

        var res = poly.decode("{\"type\":123,\"payload\":{}}");
        assertTrue(res.isError());
        assertTrue(res.errorsOrThrow().get(0)
                .contains("Failed to decode polymorphic type: IllegalArgumentException: Missing textual 'type' field"));
    }

    @Test
    void decode_error_when_discriminator_unknown() {
        Codec<TestEvent, String> poly =
                new JacksonPolymorphicByCodec<>(DISC, Map.of("alpha", ALPHA_JSON, "beta", BETA_JSON));

        var res = poly.decode("{\"type\":\"gamma\",\"payload\":{}}");
        assertTrue(res.isError());
        assertTrue(res.errorsOrThrow().get(0)
                .contains("Failed to decode polymorphic type: IllegalArgumentException: Unknown discriminator: gamma"));
    }

    @Test
    void decode_error_when_missing_payload() {
        Codec<TestEvent, String> poly =
                new JacksonPolymorphicByCodec<>(DISC, Map.of("alpha", ALPHA_JSON, "beta", BETA_JSON));

        var res = poly.decode("{\"type\":\"alpha\"}");
        assertTrue(res.isError());
        assertTrue(res.errorsOrThrow().get(0)
                .contains("Failed to decode polymorphic type: IllegalArgumentException: Missing 'payload' field"));
    }

    @Test
    void decode_propagates_sub_decoder_error_directly() {
        // Subtype codec that fails on decode (and should be returned directly)
        Codec<AlphaEvent, String> failingAlpha = new Codec<>() {
            @Override public ErrorsOr<String> encode(AlphaEvent from) { return ErrorsOr.lift("{}"); }
            @Override public ErrorsOr<AlphaEvent> decode(String to)    { return ErrorsOr.error("sub-decode-error"); }
        };

        Codec<TestEvent, String> poly =
                new JacksonPolymorphicByCodec<>(DISC, Map.of("alpha", failingAlpha));

        var res = poly.decode("{\"type\":\"alpha\",\"payload\":{}}");
        assertTrue(res.isError());
        assertEquals("sub-decode-error", res.errorsOrThrow().get(0)); // direct passthrough
    }

    @Test
    void decode_error_on_malformed_json() {
        Codec<TestEvent, String> poly =
                new JacksonPolymorphicByCodec<>(DISC, Map.of("alpha", ALPHA_JSON, "beta", BETA_JSON));

        var res = poly.decode("{ not json");
        assertTrue(res.isError());
        assertTrue(res.errorsOrThrow().get(0)
                .startsWith("Failed to decode polymorphic type:"));
    }

    @Test
    void objectMapper_is_exposed_and_ctor_with_mapper_is_covered() {
        ObjectMapper custom = new ObjectMapper().findAndRegisterModules();
        JacksonPolymorphicByCodec<TestEvent> poly =
                new JacksonPolymorphicByCodec<>(DISC, Map.of("alpha", ALPHA_JSON, "beta", BETA_JSON), custom);

        assertNotNull(poly.objectMapper());
    }
}
