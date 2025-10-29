package com.hcltech.rmg.common.azure_blob_storage;

import com.hcltech.rmg.common.azure_blob_storage.AzureBlobClient;
import com.hcltech.rmg.common.azure_blob_storage.AzureBlobConfig;
import com.hcltech.rmg.common.csv.CsvLineParser;
import com.hcltech.rmg.common.csv.CsvLookup;
import com.hcltech.rmg.common.csv.CsvLookupCollector;
import com.hcltech.rmg.common.tokens.ITokenGenerator;
import com.hcltech.rmg.common.tokens.Token;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class CsvFromAzureIntegrationTest {
    private static final String ACCOUNT   = "rmgparcelcheckpoint";
    private static final String CONTAINER = "address-lookup";
    private static final String BLOB_PATH = "postcode_suffix.csv";
    private static final String SAS_ENV_VAR = "RMG_SAS_TOKEN"; // env var with SAS
    private static final String ENDPOINT_HOST = System.getenv("RMG_ENDPOINT_HOST"); // null -> default public endpoint

    // Env var that must contain a SAS token string (with or without leading '?')

    @Test
    void downloads_csv_and_builds_lookup_roundtrips_first_row() throws Exception {
        // --- Auto-skip when SAS token is not provided ---
        String sas = System.getenv(SAS_ENV_VAR);
        assumeTrue(sas != null && !sas.isBlank(),
                "Skipping integration test: " + SAS_ENV_VAR + " not set");

        // --- Build config (host-style by default; path-style if ENDPOINT_HOST has a scheme) ---
        AzureBlobConfig cfg = new AzureBlobConfig(
                ACCOUNT,
                CONTAINER,
                BLOB_PATH,
                SAS_ENV_VAR,    // name of env var holding SAS
                ENDPOINT_HOST   // null -> "blob.core.windows.net"
        );

        // --- Token generator: just normalize SAS from env to ensure it starts with '?' ---
        ITokenGenerator tokenGen = envName -> {
            String raw = System.getenv(envName);
            if (raw == null || raw.isBlank()) throw new IllegalStateException(envName + " not set");
            return new Token(Token.Type.SAS, raw.startsWith("?") ? raw : "?" + raw);
        };

        // --- HTTP client with sane timeout ---
        HttpClient http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        // --- Stream CSV lines ---
        List<List<String>> rows = new ArrayList<>();
        CsvLineParser parser = new CsvLineParser(','); // adjust if your CSV uses a different delimiter

        try (InputStream in = AzureBlobClient.openBlobStream(cfg, tokenGen, http);
             BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {

            String line;
            while ((line = br.readLine()) != null) {
                rows.add(parser.parse(line));
            }
        }

        // --- Basic assertions on CSV shape ---
        assertTrue(rows.size() >= 2, "CSV should contain at least a header and one data row");
        List<String> header = rows.get(0);
        assertTrue(header.size() >= 2, "CSV should have at least 2 columns");

        // --- Build a lookup: key = first column; values = the rest ---
        List<String> inputCols = List.of(header.get(0));
        List<String> outputCols = header.subList(1, header.size());

        CsvLookupCollector collector = CsvLookupCollector.withCapacity(
                header, inputCols, outputCols, ".", Math.max(0, rows.size() - 1)
        );

        for (int i = 1; i < rows.size(); i++) {
            collector.accept(rows.get(i));
        }

        CsvLookup lookup = collector.build();

        // --- Round-trip check using the first data row ---
        List<String> firstData = rows.get(1);
        String firstKey = firstData.get(0);
        Map<String, Object> resolved = lookup.lookupToMap(List.of(firstKey));

        assertNotNull(resolved, "Expected a lookup result for key: " + firstKey);

        for (int j = 0; j < outputCols.size(); j++) {
            String col = outputCols.get(j);
            String expected = (j + 1 < firstData.size()) ? firstData.get(j + 1) : null;
            assertEquals(expected, resolved.get(col),
                    () -> "Mismatch at column '" + col + "' for key '" + firstKey + "'");
        }
    }
}
