package com.hcltech.rmg.config.enrich;

import com.hcltech.rmg.common.azure_blob_storage.AzureBlobConfig;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CsvFromAzureEnrichmentTest {

    private static List<List<String>> sampleInputs() {
        return List.of(List.of("$.a"), List.of("$.b"));
    }

    private static List<String> sampleOutput() {
        return List.of("$.result");
    }

    private static List<String> inputCols() {
        return List.of("a", "b");
    }

    private static List<String> outputCols() {
        return List.of("name", "code");
    }

    private static AzureBlobConfig sampleAzure() {
        return new AzureBlobConfig("acct", "cont", "path/file.csv", "sv=1&sig=abc", null);
    }

    @Test
    void constructs_ok_withAllFields_andPreservesKeyDelimiter() {
        CsvFromAzureEnrichment cfg = new CsvFromAzureEnrichment(
                sampleInputs(),
                sampleOutput(),
                sampleAzure(),
                inputCols(),
                outputCols(),
                "-"
        );

        assertEquals(sampleInputs(), cfg.inputs());
        assertEquals(sampleOutput(), cfg.output());
        assertEquals(sampleAzure().accountName(), cfg.azure().accountName()); // spot-check
        assertEquals(inputCols(), cfg.inputColumns());
        assertEquals(outputCols(), cfg.outputColumns());
        assertEquals("-", cfg.keyDelimiter());
    }

    @Test
    void keyDelimiter_defaultsToDot_whenNull() {
        CsvFromAzureEnrichment cfg = new CsvFromAzureEnrichment(
                sampleInputs(),
                sampleOutput(),
                sampleAzure(),
                inputCols(),
                outputCols(),
                null
        );
        assertEquals(".", cfg.keyDelimiter());
    }

    @Test
    void keyDelimiter_defaultsToDot_whenBlank() {
        CsvFromAzureEnrichment cfg = new CsvFromAzureEnrichment(
                sampleInputs(),
                sampleOutput(),
                sampleAzure(),
                inputCols(),
                outputCols(),
                "   "
        );
        assertEquals(".", cfg.keyDelimiter());
    }

    @Test
    void azure_mayBeNull() {
        CsvFromAzureEnrichment cfg = new CsvFromAzureEnrichment(
                sampleInputs(),
                sampleOutput(),
                null,               // allowed
                inputCols(),
                outputCols(),
                "."
        );
        assertNull(cfg.azure());
        assertEquals(".", cfg.keyDelimiter());
    }

    @Test
    void convenienceCtor_defaultsDelimiterToDot() {
        CsvFromAzureEnrichment cfg = new CsvFromAzureEnrichment(
                sampleInputs(),
                sampleOutput(),
                sampleAzure(),
                inputCols(),
                outputCols()
        );
        assertEquals(".", cfg.keyDelimiter());
        // Everything else is preserved
        assertEquals(sampleInputs(), cfg.inputs());
        assertEquals(sampleOutput(), cfg.output());
        assertEquals(inputCols(), cfg.inputColumns());
        assertEquals(outputCols(), cfg.outputColumns());
        assertNotNull(cfg.azure());
    }

    @Test
    void asDependencies_returnsSelfOnly() {
        CsvFromAzureEnrichment cfg = new CsvFromAzureEnrichment(
                sampleInputs(),
                sampleOutput(),
                sampleAzure(),
                inputCols(),
                outputCols(),
                "."
        );
        List<EnrichmentWithDependencies> deps = cfg.asDependencies();
        assertEquals(1, deps.size());
        assertSame(cfg, deps.get(0));
    }

    // ---- Validation failures ----

    @Test
    void inputs_null_throws() {
        assertThrows(NullPointerException.class, () ->
                new CsvFromAzureEnrichment(
                        null,
                        sampleOutput(),
                        sampleAzure(),
                        inputCols(),
                        outputCols(),
                        "."
                ));
    }

    @Test
    void inputs_empty_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new CsvFromAzureEnrichment(
                        List.of(),
                        sampleOutput(),
                        sampleAzure(),
                        inputCols(),
                        outputCols(),
                        "."
                ));
    }

    @Test
    void output_null_throws() {
        assertThrows(NullPointerException.class, () ->
                new CsvFromAzureEnrichment(
                        sampleInputs(),
                        null,
                        sampleAzure(),
                        inputCols(),
                        outputCols(),
                        "."
                ));
    }

    @Test
    void output_empty_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new CsvFromAzureEnrichment(
                        sampleInputs(),
                        List.of(),
                        sampleAzure(),
                        inputCols(),
                        outputCols(),
                        "."
                ));
    }

    @Test
    void inputColumns_null_throws() {
        assertThrows(NullPointerException.class, () ->
                new CsvFromAzureEnrichment(
                        sampleInputs(),
                        sampleOutput(),
                        sampleAzure(),
                        null,
                        outputCols(),
                        "."
                ));
    }

    @Test
    void inputColumns_empty_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new CsvFromAzureEnrichment(
                        sampleInputs(),
                        sampleOutput(),
                        sampleAzure(),
                        List.of(),
                        outputCols(),
                        "."
                ));
    }

    @Test
    void outputColumns_null_throws() {
        assertThrows(NullPointerException.class, () ->
                new CsvFromAzureEnrichment(
                        sampleInputs(),
                        sampleOutput(),
                        sampleAzure(),
                        inputCols(),
                        null,
                        "."
                ));
    }

    @Test
    void outputColumns_empty_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new CsvFromAzureEnrichment(
                        sampleInputs(),
                        sampleOutput(),
                        sampleAzure(),
                        inputCols(),
                        List.of(),
                        "."
                ));
    }
}
