package com.hcltech.rmg.common.apiclient;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ListQueryParamCodecTest {

    private static final Duration RT = Duration.ofMillis(1234);
    private static final String CORR = "corr-id-123";

    @Test
    void buildsSingleParam_withCommaSeparatedHumanWords() {
        var codec = new ListQueryParamCodec<String>(s -> s);
        var base = URI.create("http://example.com/search");

        var words = List.of("the", "little", "brown", "fox");
        HttpRequest req = codec.buildRequest(base, RT, "word", CORR, words);

        assertEquals("GET", req.method());
        assertEquals("/search", req.uri().getPath());

        // Expect single param with comma-separated (comma encoded as %2C)
        String expected = "word=the%2Clittle%2Cbrown%2Cfox";
        assertEquals(expected, req.uri().getRawQuery());

        // Headers include Accept (default) and traceparent
        assertEquals("application/json", req.headers().firstValue("Accept").orElse(null));
        assertEquals(CORR, req.headers().firstValue("traceparent").orElse(null));
    }

    @Test
    void encodesValues_withUrlEncoding_andFiltersNulls() {
        var codec = new ListQueryParamCodec<String>(s -> s);
        var base = URI.create("http://host/path");

        // Arrays.asList allows null; List.of does not.
        HttpRequest req = codec.buildRequest(base, RT, "q", CORR, Arrays.asList("a b", "x/y", null, "€"));

        // space -> +, slash -> %2F, euro -> %E2%82%AC; commas between values -> %2C
        assertEquals("q=a+b%2Cx%2Fy%2C%E2%82%AC", req.uri().getRawQuery());
        assertEquals(CORR, req.headers().firstValue("traceparent").orElse(null));
    }

    @Test
    void preservesExistingQuery_andAppendsSingleParam() {
        var codec = new ListQueryParamCodec<String>(s -> s);
        var base = URI.create("https://h/p?foo=bar");

        HttpRequest req = codec.buildRequest(base, RT, "word", CORR, List.of("1", "2"));
        assertEquals("foo=bar&word=1%2C2", req.uri().getRawQuery());
    }

    @Test
    void emptyOrNullValues_doNotModifyQuery() {
        var codec = new ListQueryParamCodec<String>(s -> s);
        var base = URI.create("https://h/p?x=1");

        assertEquals("x=1",
                codec.buildRequest(base, RT, "id", CORR, List.of()).uri().getRawQuery());

        assertEquals("x=1",
                codec.buildRequest(base, RT, "id", CORR, null).uri().getRawQuery());
    }

    @Test
    void defaultHeader_jsonAccept_isApplied() {
        var codec = new ListQueryParamCodec<String>(s -> s);
        var base = URI.create("https://example/path");
        var req = codec.buildRequest(base, RT, "k", CORR, List.of("v"));

        assertEquals("application/json", req.headers().firstValue("Accept").orElse(null));
        assertEquals(CORR, req.headers().firstValue("traceparent").orElse(null));
    }

    @Test
    void customHeadersFunction_isApplied() {
        var codec = new ListQueryParamCodec<String>(
                body -> body,
                (uri, in) -> Map.of(
                        "Accept", "application/json",
                        "Authorization", "Bearer abc123",
                        "X-Trace", "t-1"
                )
        );

        var base = URI.create("https://example/path");
        var req = codec.buildRequest(base, RT, "k", CORR, List.of("v"));

        assertEquals("application/json", req.headers().firstValue("Accept").orElse(null));
        assertEquals("Bearer abc123", req.headers().firstValue("Authorization").orElse(null));
        assertEquals("t-1", req.headers().firstValue("X-Trace").orElse(null));
        // codec still appends traceparent
        assertEquals(CORR, req.headers().firstValue("traceparent").orElse(null));
    }

    @Test
    void decode_appliesDecodeFunction_evenForNon2xx() {
        var codec = new ListQueryParamCodec<String>(body -> "X:" + body);
        var out = codec.decode(500, "oops",
                HttpRequest.newBuilder(URI.create("http://h")).build().headers());
        assertEquals("X:oops", out);
    }

    @Test
    void buildRequest_validatesParamName() {
        var codec = new ListQueryParamCodec<String>(s -> s);
        var base = URI.create("https://h/p");

        assertThrows(IllegalArgumentException.class, () -> codec.buildRequest(base, RT, "", CORR, List.of("v")));
        assertThrows(IllegalArgumentException.class, () -> codec.buildRequest(base, RT, "   ", CORR, List.of("v")));
        assertThrows(IllegalArgumentException.class, () -> codec.buildRequest(base, RT, null, CORR, List.of("v")));
    }

    @Test
    void constructor_validatesFunctions_only() {
        assertThrows(NullPointerException.class, () -> new ListQueryParamCodec<String>(null));
        assertThrows(NullPointerException.class, () -> new ListQueryParamCodec<String>(s -> s, null));
        // valid
        assertDoesNotThrow(() -> new ListQueryParamCodec<String>(s -> s));
    }
}
