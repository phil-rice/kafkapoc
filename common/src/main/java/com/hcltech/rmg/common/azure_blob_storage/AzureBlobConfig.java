package com.hcltech.rmg.common.azure_blob_storage;

import com.hcltech.rmg.common.IEnvGetter;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

public record AzureBlobConfig(
        String accountName,   // e.g. "myacct"
        String container,     // e.g. "data"
        String blobPath,      // e.g. "lookups/countries.csv"
        String sasEnvVar,     // optional: name of env var holding SAS
        String endpointHost   // optional: default "blob.core.windows.net" OR full base URL like "http://127.0.0.1:10000"
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

    /**
     * Builds the base blob URI (no SAS).
     * If endpointHost contains a scheme (e.g. "http://127.0.0.1:10000"), use path-style:
     *   {base}/{account}/{container}/{blob}
     * Otherwise, use host-style:
     *   https://{account}.{endpointHost}/{container}/{blob}
     */
    public URI blobUri() {
        String ep = resolvedEndpointHost();
        String encContainer = urlSeg(container);
        String encBlob = normalizeAndEncode(blobPath);

        if (ep.contains("://")) {
            // Path-style base URL
            // Ensure no trailing slash on base; then append encoded segments
            String base = stripTrailingSlash(ep);
            return URI.create(base + "/" + urlSeg(accountName) + "/" + encContainer + "/" + encBlob);
        } else {
            // Host-style
            String url = "https://" + accountName + "." + ep + "/" + encContainer + "/" + encBlob;
            return URI.create(url);
        }
    }

    /** Returns the SAS token if provided via env getter. */
    public Optional<String> resolvedSas(IEnvGetter envGetter) {
        if (sasEnvVar != null && !sasEnvVar.isBlank()) {
            String val = envGetter.get(sasEnvVar);
            if (val != null && !val.isBlank()) {
                return Optional.of(normalizeSas(val));
            }
        }
        return Optional.empty();
    }

    /** Convenience: full URI with SAS if present. */
    public URI blobUriWithSas(IEnvGetter envGetter) {
        URI base = blobUri();
        return resolvedSas(envGetter)
                .map(sas -> URI.create(base.toString() + sas))
                .orElse(base);
    }

    // ---- helpers ----

    private static String normalizeAndEncode(String path) {
        String p = path.startsWith("/") ? path.substring(1) : path;
        // Azure allows slashes as delimiters; encode each path segment
        String[] parts = p.split("/");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) sb.append('/');
            sb.append(urlSeg(parts[i]));
        }
        return sb.toString();
    }

    private static String urlSeg(String s) {
        // Encode segment, preserving safe characters
        return URLEncoder.encode(s, StandardCharsets.UTF_8)
                .replace("+", "%20"); // spaces as %20
    }

    private static String normalizeSas(String sas) {
        String trimmed = sas.trim();
        return trimmed.startsWith("?") ? trimmed : "?" + trimmed;
    }

    private static String stripTrailingSlash(String s) {
        return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }
}
