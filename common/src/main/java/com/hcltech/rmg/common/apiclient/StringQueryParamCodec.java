package com.hcltech.rmg.common.apiclient;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Codec for a single String that sends it as one query parameter:
 * GET .../path?{paramName}=value   (URL-encoded as needed)
 * Headers are set inside the codec; defaults to Accept: application/json.
 */
public final class StringQueryParamCodec<Out> implements HttpCodec<String, Out> {

    private final Function<String, Out> decodeFn;
    /**
     * Optional per-request headers: (url, input) -> headers map.
     */
    private final BiFunction<URI, String, Map<String, String>> headersFn;

    /**
     * Create with default headers: Accept: application/json
     */
    public StringQueryParamCodec(Function<String, Out> decodeFn) {
        this(decodeFn, (uri, in) -> Map.of("Accept", "application/json"));
    }

    /**
     * Create with custom headers function (e.g., to add Authorization).
     */
    public StringQueryParamCodec(Function<String, Out> decodeFn,
                                 BiFunction<URI, String, Map<String, String>> headersFn) {
        this.decodeFn = Objects.requireNonNull(decodeFn, "decodeFn");
        this.headersFn = Objects.requireNonNull(headersFn, "headersFn");
    }

    @Override
    public HttpRequest buildRequest(URI baseUrl, Duration readTimeout, String paramName, String corrId, String value) {
        // caller (client) already validates non-null/non-blank paramName; keep this defensive
        if (paramName == null || paramName.isBlank()) {
            throw new IllegalArgumentException("paramName must not be blank");
        }

        URI uriWithParams = appendParam(baseUrl, paramName, value);

        HttpRequest.Builder b = HttpRequest.newBuilder(uriWithParams)
                .timeout(readTimeout)
                .GET();

        Map<String, String> headers = new HashMap<>(headersFn.apply(baseUrl, value));
        headers.put("traceparent", corrId);
        if (headers != null && !headers.isEmpty()) {
            headers.forEach(b::header);
        }
        return b.build();
    }

    @Override
    public Out decode(int statusCode, String body, HttpHeaders headers) {
        return decodeFn.apply(body);
    }

    /**
     * Appends a single param with a URL-encoded value.
     * If value is null or blank, returns the base URI unchanged.
     */
    private static URI appendParam(URI base, String name, String value) {
        if (value == null || value.isBlank()) return base;

        String encoded = URLEncoder.encode(value, StandardCharsets.UTF_8);
        String baseStr = base.toString();
        String sep = (base.getRawQuery() == null || base.getRawQuery().isEmpty()) ? "?" : "&";
        return URI.create(baseStr + sep + name + "=" + encoded);
    }
}
