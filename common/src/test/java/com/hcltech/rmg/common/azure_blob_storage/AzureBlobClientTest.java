package com.hcltech.rmg.common.azure_blob_storage;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import com.hcltech.rmg.common.tokens.ITokenGenerator;
import com.hcltech.rmg.common.tokens.Token;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

class AzureBlobClientTest {

    // --- Test helpers --------------------------------------------------------

    /** Simple fixed token generator for tests. */
    static final class FixedTokenGenerator implements ITokenGenerator {
        private final Token token;
        FixedTokenGenerator(Token token) { this.token = token; }
        @Override public Token token(String envVariableNameOrNull) { return token; }
    }

    // Avoid messy generic casts in mocks
    @SuppressWarnings("unchecked")
    private static HttpResponse.BodyHandler<InputStream> anyInputStreamHandler() {
        return (HttpResponse.BodyHandler<InputStream>) any(HttpResponse.BodyHandler.class);
    }

    private static AzureBlobConfig baseCfg() {
        return new AzureBlobConfig(
                "acct",
                "data",
                "lookups/cities.csv",
                null,              // sasToken
                "LOCAL_BLOB_SAS",  // sasEnvVar (name irrelevant in tests)
                null               // endpointHost
        );
    }


    // --- Tests ---------------------------------------------------------------

    @Test
    void openBlobStream_success_withSas_returnsInputStream_andBuildsUrlWithSas() throws Exception {
        // Arrange
        AzureBlobConfig cfg = baseCfg();

        String sas = "?sv=2024-05-04&sp=rl&se=2030-01-01&sig=XYZ";
        ITokenGenerator tg = new FixedTokenGenerator(new Token(Token.Type.SAS, sas));

        HttpClient httpClient = mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<InputStream> httpResponse = (HttpResponse<InputStream>) mock(HttpResponse.class);

        byte[] payload = "a,b\n1,2\n".getBytes(StandardCharsets.UTF_8);
        InputStream bodyStream = new ByteArrayInputStream(payload);

        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn(bodyStream);

        ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        when(httpClient.send(requestCaptor.capture(), anyInputStreamHandler()))
                .thenReturn(httpResponse);

        // Act
        try (InputStream is = AzureBlobClient.openBlobStream(cfg, tg, httpClient)) {
            assertArrayEquals(payload, is.readAllBytes());
        }

        // Assert
        HttpRequest sent = requestCaptor.getValue();
        URI expected = URI.create(cfg.blobUri().toString() + sas);
        assertEquals(expected, sent.uri());
        assertEquals("GET", sent.method());
        // No Authorization header for SAS path
        assertTrue(sent.headers().firstValue("Authorization").isEmpty());

        verify(httpClient, times(1)).send(any(HttpRequest.class), anyInputStreamHandler());
        verifyNoMoreInteractions(httpClient);
    }

    @Test
    void openBlobStream_success_withBearer_addsAuthHeader_andVersion() throws Exception {
        // Arrange
        AzureBlobConfig cfg = new AzureBlobConfig("acct","data","lookups/cities.csv", null,null, null);

        String bearer = "Bearer eyJ.mock.token";
        ITokenGenerator tg = new FixedTokenGenerator(new Token(Token.Type.BEARER, bearer));

        HttpClient httpClient = mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<InputStream> httpResponse = (HttpResponse<InputStream>) mock(HttpResponse.class);

        byte[] payload = "x,y\n3,4\n".getBytes(StandardCharsets.UTF_8);
        InputStream bodyStream = new ByteArrayInputStream(payload);

        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn(bodyStream);

        ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        when(httpClient.send(requestCaptor.capture(), anyInputStreamHandler()))
                .thenReturn(httpResponse);

        // Act
        try (InputStream is = AzureBlobClient.openBlobStream(cfg, tg, httpClient)) {
            assertArrayEquals(payload, is.readAllBytes());
        }

        // Assert
        HttpRequest sent = requestCaptor.getValue();
        assertEquals(cfg.blobUri(), sent.uri());
        assertEquals("GET", sent.method());
        assertEquals(Optional.of(bearer), sent.headers().firstValue("Authorization"));
        assertEquals(Optional.of("2023-11-03"), sent.headers().firstValue("x-ms-version"));

        verify(httpClient, times(1)).send(any(HttpRequest.class), anyInputStreamHandler());
        verifyNoMoreInteractions(httpClient);
    }

    @Test
    void openBlobStream_non200_throwsIOException_andClosesBody() throws Exception {
        // Arrange (use SAS for simplicity)
        AzureBlobConfig cfg = baseCfg();
        ITokenGenerator tg = new FixedTokenGenerator(new Token(Token.Type.SAS, "?sv=1&sig=abc"));

        HttpClient httpClient = mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<InputStream> httpResponse = (HttpResponse<InputStream>) mock(HttpResponse.class);

        // Trackable InputStream to verify close() is called
        class ClosableStream extends InputStream {
            private final InputStream delegate;
            boolean closed = false;
            ClosableStream(InputStream delegate) { this.delegate = delegate; }
            @Override public int read() throws IOException { return delegate.read(); }
            @Override public int read(byte[] b, int off, int len) throws IOException { return delegate.read(b, off, len); }
            @Override public void close() throws IOException { closed = true; delegate.close(); }
        }

        ClosableStream bodyStream = new ClosableStream(new ByteArrayInputStream(new byte[]{1,2,3}));

        when(httpResponse.statusCode()).thenReturn(403);
        when(httpResponse.body()).thenReturn(bodyStream);
        when(httpClient.send(any(HttpRequest.class), anyInputStreamHandler())).thenReturn(httpResponse);

        // Act + Assert
        IOException ex = assertThrows(IOException.class, () -> AzureBlobClient.openBlobStream(cfg, tg, httpClient));
        assertTrue(ex.getMessage().contains("HTTP 403"));
        assertTrue(bodyStream.closed, "Response body stream should be closed on non-200");

        verify(httpClient, times(1)).send(any(HttpRequest.class), anyInputStreamHandler());
        verifyNoMoreInteractions(httpClient);
    }

    @Test
    void openBlobStream_sendThrowsIOException_propagates() throws Exception {
        AzureBlobConfig cfg = baseCfg();
        ITokenGenerator tg = new FixedTokenGenerator(new Token(Token.Type.SAS, "?sv=1&sig=abc"));

        HttpClient http = mock(HttpClient.class);
        when(http.send(any(HttpRequest.class), anyInputStreamHandler())).thenThrow(new IOException("boom"));

        IOException ex = assertThrows(IOException.class, () -> AzureBlobClient.openBlobStream(cfg, tg, http));
        assertTrue(ex.getMessage().contains("boom"));
        verify(http, times(1)).send(any(HttpRequest.class), anyInputStreamHandler());
        verifyNoMoreInteractions(http);
    }

    @Test
    void openBlobStream_sendThrowsInterruptedException_propagates() throws Exception {
        AzureBlobConfig cfg = baseCfg();
        ITokenGenerator tg = new FixedTokenGenerator(new Token(Token.Type.SAS, "?sv=1&sig=abc"));

        HttpClient http = mock(HttpClient.class);
        when(http.send(any(HttpRequest.class), anyInputStreamHandler())).thenThrow(new InterruptedException("interrupted"));

        InterruptedException ex = assertThrows(InterruptedException.class, () -> AzureBlobClient.openBlobStream(cfg, tg, http));
        assertEquals("interrupted", ex.getMessage());
        verify(http, times(1)).send(any(HttpRequest.class), anyInputStreamHandler());
        verifyNoMoreInteractions(http);
    }

    @ParameterizedTest
    @ValueSource(ints = {301, 404, 500})
    void openBlobStream_variousNon200Statuses_throwAndClose(int status) throws Exception {
        AzureBlobConfig cfg = baseCfg();
        ITokenGenerator tg = new FixedTokenGenerator(new Token(Token.Type.SAS, "?sv=1&sig=abc"));

        HttpClient http = mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<InputStream> resp = (HttpResponse<InputStream>) mock(HttpResponse.class);

        // Track close call
        class ClosableStream extends ByteArrayInputStream {
            boolean closed = false;
            ClosableStream() { super(new byte[]{1}); }
            @Override public void close() throws IOException { closed = true; super.close(); }
        }
        ClosableStream body = new ClosableStream();

        when(resp.statusCode()).thenReturn(status);
        when(resp.body()).thenReturn(body);
        when(http.send(any(HttpRequest.class), anyInputStreamHandler())).thenReturn(resp);

        IOException ex = assertThrows(IOException.class, () -> AzureBlobClient.openBlobStream(cfg, tg, http));
        assertTrue(ex.getMessage().contains("HTTP " + status));
        assertTrue(body.closed, "Body should be closed for status " + status);

        verify(http, times(1)).send(any(HttpRequest.class), anyInputStreamHandler());
        verifyNoMoreInteractions(http);
    }
}
