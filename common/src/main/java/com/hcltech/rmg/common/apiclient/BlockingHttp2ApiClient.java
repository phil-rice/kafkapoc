package com.hcltech.rmg.common.apiclient;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

/**
 * Blocking HTTP/2 client that uses HttpClientConfig and per-call url/paramName.
 */
public final class BlockingHttp2ApiClient<In, Out> implements ApiClient<In, Out> {
    private final HttpClient http;
    private final Duration readTimeout;
    private final HttpCodec<In, Out> codec;

    public BlockingHttp2ApiClient(HttpClientConfig<In, Out> config, Function<HttpClientConfig<In, Out>, HttpClient> http) {
        Objects.requireNonNull(config, "config");
        this.readTimeout = Objects.requireNonNull(config.readTimeout(), "readTimeout");
        this.codec = Objects.requireNonNull(config.codec(), "codec");
        this.http = http.apply(config);
    }

    public BlockingHttp2ApiClient(HttpClientConfig<In, Out> config) {
        this(config, c -> HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Objects.requireNonNull(c.connectTimeout(), "connectTimeout"))
                .build());
    }

    @Override
    public Out fetch(String url, String paramName, String corrId,In in) {
        Objects.requireNonNull(url, "url must not be null");
        Objects.requireNonNull(paramName, "paramName must not be null");
        if (paramName.isBlank()) {
            throw new IllegalArgumentException("paramName must not be blank");
        }

        final URI uri = URI.create(url);

        try {
            var req = codec.buildRequest(uri, readTimeout, paramName, corrId,in);
            var res = http.send(req, HttpResponse.BodyHandlers.ofString());

            final int sc = res.statusCode();

            // Treat 404 (and optionally 204) as empty result
            if (sc == 404 || sc == 204) {
                return null;
            }

            if (sc >= 200 && sc < 300) {
                return codec.decode(sc, res.body(), res.headers());
            }

            throw new RuntimeException("HTTP " + sc + " calling " + url + " body=" + truncate(res.body(), 512));

        } catch (HttpTimeoutException e) {
            throw new RuntimeException("Timeout calling " + url, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while calling " + url, e);
        } catch (Exception e) {
            throw new RuntimeException("Error calling " + url, e);
        }
    }

    private static String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }
}
