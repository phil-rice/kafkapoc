package com.hcltech.rmg.common.azure_blob_storage;

import com.hcltech.rmg.common.IEnvGetter;

import java.net.URI;
import java.util.Objects;
import java.util.Optional;

public record AzureBlobConfig(
        String accountName,   // e.g. "myacct"
        String container,     // e.g. "data"
        String blobPath,      // e.g. "lookups/countries.csv"
        String sasToken,      // optional: env variable for raw SAS (with or without '?')
        String sasEnvVar,     // optional: name of env var holding SAS
        String endpointHost   // optional: default "blob.core.windows.net"
) {

    public AzureBlobConfig {
        Objects.requireNonNull(accountName, "accountName");
        Objects.requireNonNull(container, "container");
        Objects.requireNonNull(blobPath, "blobPath");
        if (accountName.isBlank()) throw new IllegalArgumentException("accountName cannot be blank");
        if (container.isBlank()) throw new IllegalArgumentException("container cannot be blank");
        if (blobPath.isBlank()) throw new IllegalArgumentException("blobPath cannot be blank");
    }

    public String resolvedEndpointHost() {
        return (endpointHost == null || endpointHost.isBlank())
                ? "blob.core.windows.net"
                : endpointHost;
    }

    /** Builds the base blob URI (no SAS). */
    public URI blobUri() {
        String url = "https://" + accountName + "." + resolvedEndpointHost()
                + "/" + container + "/" + normalize(blobPath);
        return URI.create(url);
    }

    /** Returns the SAS token if provided directly or via env getter. */
    public Optional<String> resolvedSas(IEnvGetter envGetter) {
        if (sasToken != null && !sasToken.isBlank()) {
            return Optional.of(normalizeSas(sasToken));
        }
        if (sasEnvVar != null && !sasEnvVar.isBlank()) {
            String val = envGetter.get(sasEnvVar);
            if (val != null && !val.isBlank()) {
                return Optional.of(normalizeSas(val));
            }
        }
        return Optional.empty();
    }

    private static String normalize(String path) {
        return path.startsWith("/") ? path.substring(1) : path;
    }

    private static String normalizeSas(String sas) {
        return sas.startsWith("?") ? sas : "?" + sas;
    }
}
