package com.hcltech.rmg.common.apiclient;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Codec for List&lt;String&gt; that sends them as a single comma-separated query parameter:
 * GET .../path?{paramName}=a,b,c  (URL-encoded as needed, comma encoded as %2C)
 * Headers are set inside the codec; defaults to Accept: application/json.
 */
public final class ListQueryParamCodec<Out> implements HttpCodec<List<String>, Out> {

    private final Function<String, Out> decodeFn;
    /**
     * Optional per-request headers: (url, input) -> headers map.
     */
    private final BiFunction<URI, List<String>, Map<String, String>> headersFn;

    /**
     * Create with default headers: Accept: application/json
     */
    public ListQueryParamCodec(Function<String, Out> decodeFn) {
        this(decodeFn, (uri, in) -> Map.of("Accept", "application/json"));
    }

    /**
     * Create with custom headers function (e.g., to add Authorization).
     */
    public ListQueryParamCodec(Function<String, Out> decodeFn,
                               BiFunction<URI, List<String>, Map<String, String>> headersFn) {
        this.decodeFn = Objects.requireNonNull(decodeFn, "decodeFn");
        this.headersFn = Objects.requireNonNull(headersFn, "headersFn");
    }

    @Override
    public HttpRequest buildRequest(URI baseUrl, Duration readTimeout, String paramName, String corrId, List<String> values) {
        if (paramName == null || paramName.isBlank()) {
            throw new IllegalArgumentException("paramName must not be blank");
        }

        URI uriWithParams = appendCommaSeparatedParam(baseUrl, paramName, values);

        HttpRequest.Builder b = HttpRequest.newBuilder(uriWithParams)
                .timeout(readTimeout)
                .GET();

        Map<String, String> headers = new HashMap<>(headersFn.apply(baseUrl, values));
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
     * Appends a single param with a comma-separated (URL-encoded) value.
     * Avoids double-encoding by constructing the final URI from the base URI string.
     */
    private static URI appendCommaSeparatedParam(URI base, String name, List<String> values) {
        if (values == null || values.isEmpty()) return base;

        // Encode each value individually, then join with encoded comma (%2C)
        String encodedJoined = values.stream()
                .filter(Objects::nonNull)
                .map(v -> URLEncoder.encode(v, StandardCharsets.UTF_8))
                .collect(Collectors.joining("%2C"));

        if (encodedJoined.isEmpty()) return base;

        String baseStr = base.toString();
        String sep = (base.getRawQuery() == null || base.getRawQuery().isEmpty()) ? "?" : "&";
        return URI.create(baseStr + sep + name + "=" + encodedJoined);
    }
}
