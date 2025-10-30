package com.hcltech.rmg.config.enrich;

import com.hcltech.rmg.common.azure_blob_storage.AzureBlobConfig;

import java.util.List;
import java.util.Objects;

/**
 * Pure configuration for CSV-based enrichment (no I/O).
 * <p>
 * Fields:
 * - inputs: JSON paths used to build the composite key (order matters)
 * - output: JSON path where the enrichment payload (a Map&lt;outputColumn, value&gt;) is written
 * - azure: optional Azure blob configuration (may be null if lookup is provided elsewhere)
 * - inputColumns: CSV columns that form the composite key (ordering matters)
 * - outputColumns: CSV columns that name the payload values (ordering matters)
 * - keyDelimiter: delimiter used to join input values into the composite key (defaults to ".")
 */
public record CsvFromAzureEnrichment(
        List<List<String>> inputs,
        List<String> output,
        AzureBlobConfig azure,   // may be null (metadata / or external loader)
        List<String> inputColumns,
        List<String> outputColumns,
        String keyDelimiter
) implements EnrichmentAspect, EnrichmentWithDependencies {

    public CsvFromAzureEnrichment {
        Objects.requireNonNull(inputs, "inputs");
        Objects.requireNonNull(output, "output");
        Objects.requireNonNull(inputColumns, "inputColumns");
        Objects.requireNonNull(outputColumns, "outputColumns");

        // default delimiter if null/blank
        if (keyDelimiter == null || keyDelimiter.isBlank()) {
            keyDelimiter = ".";
        }

        if (inputs.isEmpty()) throw new IllegalArgumentException("inputs cannot be empty");
        if (output.isEmpty()) throw new IllegalArgumentException("output cannot be empty");
        if (inputColumns.isEmpty()) throw new IllegalArgumentException("inputColumns cannot be empty");
        if (outputColumns.isEmpty()) throw new IllegalArgumentException("outputColumns cannot be empty");
        // azure may be null by design
    }

    /**
     * Convenience overload with default key delimiter "."
     */
    public CsvFromAzureEnrichment(
            List<List<String>> inputs,
            List<String> output,
            AzureBlobConfig azure,
            List<String> inputColumns,
            List<String> outputColumns
    ) {
        this(inputs, output, azure, inputColumns, outputColumns, ".");
    }

    @Override
    public List<EnrichmentWithDependencies> asDependencies() {
        return List.of(this);
    }
}
