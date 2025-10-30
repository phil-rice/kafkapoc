package com.hcltech.rmg.common.apiclient;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class StringQueryParamCodecTest {

    private static final String CORR = "corr-id-123";

    private static StringQueryParamCodec<String> defaultCodec() {
        return new StringQueryParamCodec<>(body -> body);
    }

    @Test
    void buildRequest_addsParam_whenValuePresent() {
        var codec = defaultCodec();
        var base = URI.create("https://api.example.com/v1/search");
        var req = codec.buildRequest(base, Duration.ofSeconds(3), "q", CORR, "hello");

        assertEquals("GET", req.method());
        assertTrue(req.timeout().isPresent());
        assertEquals(Duration.ofSeconds(3), req.timeout().get());

        // param added (no special encoding needed for "hello")
        assertEquals("https://api.example.com/v1/search?q=hello", req.uri().toString());

        // headers: default Accept + traceparent
        assertEquals("application/json", req.headers().firstValue("Accept").orElse(null));
        assertEquals(CORR, req.headers().firstValue("traceparent").orElse(null));
    }

    @Test
    void buildRequest_encodesReservedCharacters() {
        var codec = defaultCodec();
        var base = URI.create("https://api.example.com/v1/search");
        var req = codec.buildRequest(base, Duration.ofSeconds(3), "q", CORR, "a b,c/汉字");

        // ' ' -> '+', ',' -> %2C, '/' -> %2F, non-ASCII percent-encoded
        String uri = req.uri().toString();
        assertTrue(uri.contains("q=a+b%2Cc%2F%E6%B1%89%E5%AD%97"), uri);

        assertEquals(CORR, req.headers().firstValue("traceparent").orElse(null));
    }

    @Test
    void buildRequest_appendsWithAmpersand_whenQueryAlreadyPresent() {
        var codec = defaultCodec();
        var base = URI.create("https://api.example.com/v1/search?foo=1");
        var req = codec.buildRequest(base, Duration.ofSeconds(1), "q", CORR, "x");

        assertEquals("https://api.example.com/v1/search?foo=1&q=x", req.uri().toString());
    }

    @Test
    void buildRequest_skipsParam_whenValueNull() {
        var codec = defaultCodec();
        var base = URI.create("https://api.example.com/v1/search?foo=1");
        var req = codec.buildRequest(base, Duration.ofSeconds(1), "q", CORR, null);

        assertEquals(base.toString(), req.uri().toString());
    }

    @Test
    void buildRequest_skipsParam_whenValueBlank() {
        var codec = defaultCodec();
        var base = URI.create("https://api.example.com/v1/search?foo=1");
        var req = codec.buildRequest(base, Duration.ofSeconds(1), "q", CORR, "   ");

        assertEquals(base.toString(), req.uri().toString());
    }

    @Test
    void defaultHeaders_setsAcceptApplicationJson_andTraceparent() {
        var codec = defaultCodec();
        var req = codec.buildRequest(URI.create("https://x"), Duration.ofMillis(500), "q", CORR, "v");

        assertEquals("application/json", req.headers().firstValue("Accept").orElse(null));
        assertEquals(CORR, req.headers().firstValue("traceparent").orElse(null));
    }

    @Test
    void customHeaders_areApplied_andTraceparentAdded() {
        var codec = new StringQueryParamCodec<>(
                body -> body,
                (uri, in) -> Map.of("Accept", "application/json", "Authorization", "Bearer ABC")
        );

        var req = codec.buildRequest(URI.create("https://x"), Duration.ofSeconds(2), "q", CORR, "v");
        assertEquals("application/json", req.headers().firstValue("Accept").orElse(null));
        assertEquals("Bearer ABC", req.headers().firstValue("Authorization").orElse(null));
        assertEquals(CORR, req.headers().firstValue("traceparent").orElse(null));
    }

    @Test
    void emptyHeadersMap_isAllowed_andStillAddsTraceparent() {
        var codec = new StringQueryParamCodec<>(
                body -> body,
                (uri, in) -> Map.of() // empty map (NOT null) → fine
        );
        var req = codec.buildRequest(URI.create("https://x"), Duration.ofSeconds(1), "q", CORR, "v");

        assertTrue(req.headers().firstValue("Accept").isEmpty(), "Accept should be absent when headersFn returns empty map");
        assertEquals(CORR, req.headers().firstValue("traceparent").orElse(null));
    }

    @Test
    void decode_passesBodyToDecodeFn() {
        var codec = new StringQueryParamCodec<>(body -> "len=" + (body == null ? "null" : body.length()));
        var out = codec.decode(200, "hello",
                HttpRequest.newBuilder().uri(URI.create("https://x")).build().headers());
        assertEquals("len=5", out);
    }

    @Nested
    class BuildValidation {
        @Test
        void blankParamName_rejected() {
            var codec = defaultCodec();
            var base = URI.create("https://x");
            assertThrows(IllegalArgumentException.class, () ->
                    codec.buildRequest(base, Duration.ofMillis(1), "", CORR, "v"));
            assertThrows(IllegalArgumentException.class, () ->
                    codec.buildRequest(base, Duration.ofMillis(1), "   ", CORR, "v"));
        }

        @Test
        void nullParamName_rejected() {
            var codec = defaultCodec();
            assertThrows(IllegalArgumentException.class, () ->
                    codec.buildRequest(URI.create("https://x"), Duration.ofMillis(1), null, CORR, "v"));
        }

        @Test
        void nullDecodeFn_rejected() {
            assertThrows(NullPointerException.class, () -> new StringQueryParamCodec<>(null));
        }

        @Test
        void nullHeadersFn_rejectedByCtor() {
            assertThrows(NullPointerException.class,
                    () -> new StringQueryParamCodec<>(body -> body, null));
        }
    }
}
