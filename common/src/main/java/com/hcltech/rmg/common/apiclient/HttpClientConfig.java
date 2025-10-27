package com.hcltech.rmg.common.apiclient;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Configuration for an HTTP client: timeouts, codec, and header builder.
 */
public record HttpClientConfig<In, Out>(
        Duration connectTimeout,
        Duration readTimeout,
        HttpCodec<In, Out> codec,
        BiFunction<URI, In, Map<String, String>> headersFn // credentials, Accept, tracing, etc.
) {
    public HttpClientConfig {
        Objects.requireNonNull(connectTimeout, "connectTimeout");
        Objects.requireNonNull(readTimeout, "readTimeout");
        Objects.requireNonNull(codec, "codec");
        Objects.requireNonNull(headersFn, "headersFn");
    }

    /** Default headers function: {@code Accept: application/json}. */
    public static <In> BiFunction<URI, In, Map<String, String>> defaultHeadersFn() {
        return (uri, in) -> Map.of("Accept", "application/json");
    }

    /** Convenience: Accept: application/json with no other headers. */
    public static <In, Out> HttpClientConfig<In, Out> withJsonAccept(
            Duration connectTimeout, Duration readTimeout, HttpCodec<In, Out> codec
    ) {
        return new HttpClientConfig<>(connectTimeout, readTimeout, codec, defaultHeadersFn());
    }
}
