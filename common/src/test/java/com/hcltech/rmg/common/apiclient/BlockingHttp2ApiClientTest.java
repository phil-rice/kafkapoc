package com.hcltech.rmg.common.apiclient;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class BlockingHttp2ApiClientTest {

    private static HttpServer server;
    private static int port;

    // capture latest request details for assertions
    private static final AtomicReference<String> lastQuery = new AtomicReference<>();
    private static final AtomicReference<String> lastTraceparent = new AtomicReference<>();

    @BeforeAll
    static void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress(0), 0); // ephemeral port
        port = server.getAddress().getPort();

        server.createContext("/ok", ex -> { capture(ex); respond(ex, 200, "OK-RESPONSE"); });
        server.createContext("/fail", ex -> { capture(ex); respond(ex, 500, "FAIL-RESPONSE"); });
        server.createContext("/missing", ex -> { capture(ex); respond(ex, 404, "NOT-FOUND"); });
        server.createContext("/slow", ex -> {
            capture(ex);
            try { Thread.sleep(800); } catch (InterruptedException ignored) {}
            respond(ex, 200, "SLOW-RESPONSE");
        });

        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();
    }

    @AfterAll
    static void stopServer() {
        if (server != null) server.stop(0);
    }

    private static String baseUrl() { return "http://127.0.0.1:" + port; }

    // ----------------------------------------------------------------------

    @Test
    void stringCodec_success_decodesBody_and_setsTraceparent_and_encodesParam() {
        var codec = new StringQueryParamCodec<String>(body -> "decoded:" + body);
        var cfg = new HttpClientConfig<>(
                Duration.ofMillis(200),
                Duration.ofMillis(500),
                codec,
                HttpClientConfig.<String>defaultHeadersFn() // required by record
        );
        var client = new BlockingHttp2ApiClient<>(cfg);

        String corrId = "corr-123";
        String out = client.fetch(baseUrl() + "/ok", "q", corrId, "hello world");

        assertEquals("decoded:OK-RESPONSE", out);
        // Java URLEncoder encodes spaces as '+'
        assertEquals("q=hello+world", lastQuery.get());
        assertEquals(corrId, lastTraceparent.get());
    }

    @Test
    void listCodec_success_buildsCommaSeparatedParam_and_setsTraceparent() {
        var codec = new ListQueryParamCodec<String>(body -> body);
        var cfg = new HttpClientConfig<>(
                Duration.ofMillis(200),
                Duration.ofMillis(500),
                codec,
                HttpClientConfig.<List<String>>defaultHeadersFn()
        );
        var client = new BlockingHttp2ApiClient<>(cfg);

        String corrId = "corr-xyz";
        var values = List.of("alpha", "b c"); // contains space to check encoding
        String out = client.fetch(baseUrl() + "/ok", "q", corrId, values);

        assertEquals("OK-RESPONSE", out);
        // Each value encoded individually then joined with %2C → "alpha%2Cb+c"
        assertEquals("q=alpha%2Cb+c", lastQuery.get());
        assertEquals(corrId, lastTraceparent.get());
    }

    @Test
    void non2xx_throwsRuntimeException_withHttpStatusInMessage() {
        var codec = new StringQueryParamCodec<String>(body -> body);
        var cfg = new HttpClientConfig<>(
                Duration.ofMillis(200),
                Duration.ofMillis(500),
                codec,
                HttpClientConfig.<String>defaultHeadersFn()
        );
        var client = new BlockingHttp2ApiClient<>(cfg);

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> client.fetch(baseUrl() + "/fail", "q", "corr", "x"));
        String msg = String.valueOf(ex.getMessage());

    }

    @Test
    void notFound_returnsNull() {
        var codec = new StringQueryParamCodec<String>(body -> body);
        var cfg = new HttpClientConfig<>(
                Duration.ofMillis(200),
                Duration.ofMillis(500),
                codec,
                HttpClientConfig.<String>defaultHeadersFn()
        );
        var client = new BlockingHttp2ApiClient<>(cfg);

        String result = client.fetch(baseUrl() + "/missing", "q", "corr", "irrelevant");
        assertNull(result, "404 should return null, not throw");
    }

    @Test
    void timeout_wrapsHttpTimeoutException_withHelpfulMessage() {
        var codec = new StringQueryParamCodec<String>(body -> body);
        var cfg = new HttpClientConfig<>(
                Duration.ofMillis(200),
                Duration.ofMillis(200), // shorter than /slow delay
                codec,
                HttpClientConfig.<String>defaultHeadersFn()
        );
        var client = new BlockingHttp2ApiClient<>(cfg);

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> client.fetch(baseUrl() + "/slow", "q", "corr", "x"));
        assertTrue(ex.getMessage().toLowerCase().contains("timeout"),
                "Expected timeout in message: " + ex.getMessage());
        assertNotNull(ex.getCause());
        assertEquals(java.net.http.HttpTimeoutException.class, ex.getCause().getClass());
    }

    @Test
    void blankParamName_throwsIAE_beforeNetworkCall() {
        var codec = new StringQueryParamCodec<String>(body -> body);
        var cfg = new HttpClientConfig<>(
                Duration.ofMillis(200),
                Duration.ofMillis(500),
                codec,
                HttpClientConfig.<String>defaultHeadersFn()
        );
        var client = new BlockingHttp2ApiClient<>(cfg);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> client.fetch(baseUrl() + "/ok", "  ", "corr", "x"));
        assertTrue(ex.getMessage().contains("paramName must not be blank"));
    }

    // ----------------------------------------------------------------------

    private static void capture(HttpExchange ex) {
        lastQuery.set(ex.getRequestURI().getRawQuery()); // raw (already-encoded) query
        lastTraceparent.set(ex.getRequestHeaders().getFirst("traceparent"));
    }

    private static void respond(HttpExchange exchange, int code, String body) throws IOException {
        byte[] bytes = body.getBytes();
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
