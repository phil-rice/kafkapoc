package com.hcltech.rmg.common.apiclient;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HttpClientConfigTest {

    @Test
    void nullConnectTimeout_rejected() {
        assertThrows(NullPointerException.class,
                () -> new HttpClientConfig<>(null, Duration.ofMillis(100), new NoopCodec<>(), (u, in) -> Map.of()));
    }

    @Test
    void nullReadTimeout_rejected() {
        assertThrows(NullPointerException.class,
                () -> new HttpClientConfig<>(Duration.ofMillis(100), null, new NoopCodec<>(), (u, in) -> Map.of()));
    }

    @Test
    void nullCodec_rejected() {
        assertThrows(NullPointerException.class,
                () -> new HttpClientConfig<>(Duration.ofMillis(100), Duration.ofMillis(100), null, (u, in) -> Map.of()));
    }

    @Test
    void nullHeadersFn_rejected() {
        assertThrows(NullPointerException.class,
                () -> new HttpClientConfig<>(Duration.ofMillis(100), Duration.ofMillis(100), new NoopCodec<>(), null));
    }

    @Test
    void valid_constructs() {
        assertDoesNotThrow(() ->
                new HttpClientConfig<>(Duration.ofMillis(50), Duration.ofMillis(100), new NoopCodec<>(), (u, in) -> Map.of()));
    }

    @Test
    void withJsonAccept_factory_setsAcceptHeaderOnly() {
        var cfg = HttpClientConfig.withJsonAccept(Duration.ofMillis(10), Duration.ofMillis(20), new NoopCodec<>());
        assertNotNull(cfg);
        assertEquals(Duration.ofMillis(10), cfg.connectTimeout());
        assertEquals(Duration.ofMillis(20), cfg.readTimeout());
        assertNotNull(cfg.codec());

        Map<String,String> headers = cfg.headersFn().apply(URI.create("https://example.com/x"), "ignored");
        assertEquals("application/json", headers.get("Accept"));
        assertEquals(1, headers.size(), "withJsonAccept should only set Accept header");
    }

    /** Minimal codec for config tests only. */
    private static final class NoopCodec<T> implements HttpCodec<T, T> {
        @Override
        public HttpRequest buildRequest(URI url, Duration readTimeout, String paramName, String corrId, T in) {
            throw new UnsupportedOperationException("not used");
        }
        @Override
        public T decode(int status, String body, HttpHeaders headers) {
            throw new UnsupportedOperationException("not used");
        }
    }
}
