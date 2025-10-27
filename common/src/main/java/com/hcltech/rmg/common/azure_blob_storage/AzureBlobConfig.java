package com.hcltech.rmg.common.azure_blob_storage;

import java.net.URI;
import java.util.Objects;

public record AzureBlobConfig(
        String accountName,      // e.g. "myacct"
        String container,        // e.g. "data"
        String blobPath,         // e.g. "lookups/countries.csv"
        String sasToken,         // e.g. "?sv=2024-05-04&ss=b&srt=...&se=...&sp=...&sig=..."
        String endpointHost      // optional; default "blob.core.windows.net"
) {
    public AzureBlobConfig {
        Objects.requireNonNull(accountName, "accountName");
        Objects.requireNonNull(container, "container");
        Objects.requireNonNull(blobPath, "blobPath");
        Objects.requireNonNull(sasToken, "sasToken");

        if (accountName.isBlank()) throw new IllegalArgumentException("accountName cannot be blank");
        if (container.isBlank()) throw new IllegalArgumentException("container cannot be blank");
        if (blobPath.isBlank()) throw new IllegalArgumentException("blobPath cannot be blank");
        if (sasToken.isBlank()) throw new IllegalArgumentException("sasToken cannot be blank");
    }

    /** Returns the canonical blob endpoint host, defaulting to Azure public cloud. */
    public String resolvedEndpointHost() {
        return (endpointHost == null || endpointHost.isBlank())
                ? "blob.core.windows.net"
                : endpointHost;
    }

    /** Builds the full signed URI you can GET directly. */
    public URI signedBlobUri() {
        String sas = sasToken.startsWith("?") ? sasToken : "?" + sasToken;
        String url = "https://" + accountName + "." + resolvedEndpointHost()
                   + "/" + container + "/" + normalize(blobPath) + sas;
        return URI.create(url);
    }

    private static String normalize(String path) {
        // strip any leading slash to avoid double slashes
        return path.startsWith("/") ? path.substring(1) : path;
    }
}
