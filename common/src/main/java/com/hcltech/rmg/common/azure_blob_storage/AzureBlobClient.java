package com.hcltech.rmg.common.azure_blob_storage;


import com.hcltech.rmg.common.tokens.ITokenGenerator;
import com.hcltech.rmg.common.tokens.Token;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * Minimal HTTP-based Azure Blob reader supporting both SAS and Managed Identity tokens.
 * <p>
 * - If {@link Token.Type#SAS}, the SAS token is appended to the URL.
 * - If {@link Token.Type#BEARER}, it is sent as an Authorization header.
 */
public final class AzureBlobClient {

    private AzureBlobClient() {}

    /**
     * Opens an InputStream to the blob described by the given config.
     * <p>
     * The token is supplied by an injected {@link ITokenGenerator}.
     */
    public static InputStream openBlobStream(AzureBlobConfig cfg,
                                             ITokenGenerator tokenGenerator)
            throws IOException, InterruptedException {
        return openBlobStream(cfg, tokenGenerator, HttpClient.newHttpClient());
    }

    public static InputStream openBlobStream(AzureBlobConfig cfg,
                                             ITokenGenerator tokenGenerator,
                                             HttpClient httpClient)
            throws IOException, InterruptedException {

        var token = tokenGenerator.token(cfg.sasEnvVar());
        var tokenType = token.type();
        var tokenValue = token.value();

        URI uri;
        HttpRequest.Builder requestBuilder;

        if (tokenType == Token.Type.SAS) {
            // Append SAS token to the blob URL (no header)
            uri = URI.create(cfg.blobUri().toString() + tokenValue);
            requestBuilder = HttpRequest.newBuilder().uri(uri).GET();
        } else {
            // Use Bearer token header
            uri = cfg.blobUri();
            requestBuilder = HttpRequest.newBuilder()
                    .uri(uri)
                    .header("Authorization", tokenValue)
                    .header("x-ms-version", "2023-11-03")
                    .GET();
        }

        var response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofInputStream());
        int status = response.statusCode();
        if (status != 200) {
            response.body().close();
            throw new IOException("Failed to fetch blob " + uri + ": HTTP " + status);
        }

        return response.body();
    }
}
