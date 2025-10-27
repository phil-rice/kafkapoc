package com.hcltech.rmg.common.azure_blob_storage;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

class AzureBlobClientTest {

    // Helper to avoid messy generic casts everywhere
    @SuppressWarnings("unchecked")
    private static HttpResponse.BodyHandler<InputStream> anyInputStreamHandler() {
        return (HttpResponse.BodyHandler<InputStream>) any(HttpResponse.BodyHandler.class);
    }

    @Test
    void openBlobStream_success_returnsInputStream() throws Exception {
        // Arrange
        AzureBlobConfig cfg = new AzureBlobConfig(
                "acct",
                "data",
                "lookups/cities.csv",
                "sv=2024-05-04&sp=rl&se=2030-01-01&sig=XYZ",
                null
        );

        HttpClient httpClient = mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<InputStream> httpResponse = (HttpResponse<InputStream>) mock(HttpResponse.class);

        byte[] payload = "a,b\n1,2\n".getBytes(StandardCharsets.UTF_8);
        InputStream bodyStream = new ByteArrayInputStream(payload);

        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn(bodyStream);

        // Capture the request to assert URI and method
        ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);

        when(httpClient.send(requestCaptor.capture(), anyInputStreamHandler()))
                .thenReturn(httpResponse);

        // Act
        try (InputStream is = AzureBlobClient.openBlobStream(cfg, httpClient)) {
            // Assert: content is readable
            byte[] buf = is.readAllBytes();
            assertArrayEquals(payload, buf);
        }

        // Assert: request built to expected URI via cfg.signedBlobUri()
        var sentRequest = requestCaptor.getValue();
        assertEquals(cfg.signedBlobUri(), sentRequest.uri());
        // Method should be GET
        assertEquals("GET", sentRequest.method());

        verify(httpClient, times(1)).send(any(HttpRequest.class), anyInputStreamHandler());
        verifyNoMoreInteractions(httpClient);
    }

    @Test
    void openBlobStream_non200_throwsIOException_andClosesBody() throws Exception {
        // Arrange
        AzureBlobConfig cfg = new AzureBlobConfig(
                "acct",
                "data",
                "lookups/cities.csv",
                "sv=2024-05-04&sp=rl&se=2030-01-01&sig=XYZ",
                null
        );

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
        IOException ex = assertThrows(IOException.class, () -> AzureBlobClient.openBlobStream(cfg, httpClient));
        assertTrue(ex.getMessage().contains("HTTP 403"));
        assertTrue(bodyStream.closed, "Response body stream should be closed on non-200");

        verify(httpClient, times(1)).send(any(HttpRequest.class), anyInputStreamHandler());
        verifyNoMoreInteractions(httpClient);
    }

    @Test
    void openBlobStream_sendThrowsIOException_propagates() throws Exception {
        AzureBlobConfig cfg = new AzureBlobConfig("acct","data","x.csv","sv=1&sig=abc", null);
        HttpClient http = mock(HttpClient.class);
        when(http.send(any(HttpRequest.class), anyInputStreamHandler())).thenThrow(new IOException("boom"));

        IOException ex = assertThrows(IOException.class, () -> AzureBlobClient.openBlobStream(cfg, http));
        assertTrue(ex.getMessage().contains("boom"));
        verify(http, times(1)).send(any(HttpRequest.class), anyInputStreamHandler());
        verifyNoMoreInteractions(http);
    }

    @Test
    void openBlobStream_sendThrowsInterruptedException_propagates() throws Exception {
        AzureBlobConfig cfg = new AzureBlobConfig("acct","data","x.csv","sv=1&sig=abc", null);
        HttpClient http = mock(HttpClient.class);
        when(http.send(any(HttpRequest.class), anyInputStreamHandler())).thenThrow(new InterruptedException("interrupted"));

        InterruptedException ex = assertThrows(InterruptedException.class, () -> AzureBlobClient.openBlobStream(cfg, http));
        assertEquals("interrupted", ex.getMessage());
        verify(http, times(1)).send(any(HttpRequest.class), anyInputStreamHandler());
        verifyNoMoreInteractions(http);
    }

    @ParameterizedTest
    @ValueSource(ints = {301, 404, 500})
    void openBlobStream_variousNon200Statuses_throwAndClose(int status) throws Exception {
        AzureBlobConfig cfg = new AzureBlobConfig("acct","data","x.csv","sv=1&sig=abc", null);
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

        IOException ex = assertThrows(IOException.class, () -> AzureBlobClient.openBlobStream(cfg, http));
        assertTrue(ex.getMessage().contains("HTTP " + status));
        assertTrue(body.closed, "Body should be closed for status " + status);

        verify(http, times(1)).send(any(HttpRequest.class), anyInputStreamHandler());
        verifyNoMoreInteractions(http);
    }
}
