package com.hcltech.rmg.common.azure_blob_storage;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * Minimal HTTP client for reading Azure blobs via SAS URL.
 * Uses the JDK built-in HttpClient and returns an InputStream of the blob data.
 */
public final class AzureBlobClient {

    private AzureBlobClient() {}

    /**
     * Opens an InputStream to the blob described by the given config.
     *
     * @param cfg AzureBlobConfig containing the full SAS URL information
     * @return InputStream of blob contents (caller must close it)
     */
    public static InputStream openBlobStream(AzureBlobConfig cfg) throws IOException, InterruptedException {
        return openBlobStream(cfg, HttpClient.newHttpClient());
    }
    public static InputStream openBlobStream(AzureBlobConfig cfg, HttpClient httpClient) throws IOException, InterruptedException {
        URI uri = cfg.signedBlobUri();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .GET()
                .build();

        // Use BodyHandlers.ofInputStream to lazily stream data
        HttpResponse<InputStream> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

        int status = response.statusCode();
        if (status != 200) {
            // Drain/close before throwing, to free the connection
            response.body().close();
            throw new IOException("Failed to fetch blob " + uri + ": HTTP " + status);
        }

        return response.body();
    }
}
