package com.hcltech.rmg.config.enrich;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CsvEnrichmentConfigTest {

    @Test
    void happyPath_nullCsvFileName_andDefaultDelimiter() {
        CsvEnrichment cfg = new CsvEnrichment(
                List.of(List.of("a"), List.of("b")), // inputs
                List.of("out"),                      // output path
                null,                                 // csvFileName is optional
                List.of("a", "b"),                    // input columns
                List.of("X", "Y"),                    // output columns
                null                                   // keyDelimiter -> should default to "."
        );

        assertEquals(List.of(List.of("a"), List.of("b")), cfg.inputs());
        assertEquals(List.of("out"), cfg.output());
        assertNull(cfg.csvFileName());
        assertEquals(List.of("a", "b"), cfg.inputColumns());
        assertEquals(List.of("X", "Y"), cfg.outputColumns());
        assertEquals(".", cfg.keyDelimiter(), "Default delimiter should be '.'");

        // asDependencies returns itself
        assertEquals(1, cfg.asDependencies().size());
        assertSame(cfg, cfg.asDependencies().get(0));
    }

    @Test
    void explicitDelimiterIsRespected() {
        CsvEnrichment cfg = new CsvEnrichment(
                List.of(List.of("a"), List.of("b")),
                List.of("out"),
                "CsvResourceLoaderTest/sample.csv",
                List.of("a", "b"),
                List.of("X", "Y"),
                "-" // explicit delimiter
        );
        assertEquals("-", cfg.keyDelimiter());
    }

    @Test
    void blankDelimiterFallsBackToDefault() {
        CsvEnrichment cfg = new CsvEnrichment(
                List.of(List.of("a"), List.of("b")),
                List.of("out"),
                null,
                List.of("a", "b"),
                List.of("X", "Y"),
                "   " // blank
        );
        assertEquals(".", cfg.keyDelimiter(), "Blank delimiter should default to '.'");
    }

    @Test
    void inputs_mustNotBeNullOrEmpty() {
        assertThrows(NullPointerException.class, () ->
                new CsvEnrichment(null, List.of("out"), null, List.of("a"), List.of("X"), "."));
        assertThrows(IllegalArgumentException.class, () ->
                new CsvEnrichment(List.of(), List.of("out"), null, List.of("a"), List.of("X"), "."));
    }

    @Test
    void output_mustNotBeNullOrEmpty() {
        assertThrows(NullPointerException.class, () ->
                new CsvEnrichment(List.of(List.of("a")), null, null, List.of("a"), List.of("X"), "."));
        assertThrows(IllegalArgumentException.class, () ->
                new CsvEnrichment(List.of(List.of("a")), List.of(), null, List.of("a"), List.of("X"), "."));
    }

    @Test
    void inputColumns_mustNotBeNullOrEmpty() {
        assertThrows(NullPointerException.class, () ->
                new CsvEnrichment(List.of(List.of("a")), List.of("out"), null, null, List.of("X"), "."));
        assertThrows(IllegalArgumentException.class, () ->
                new CsvEnrichment(List.of(List.of("a")), List.of("out"), null, List.of(), List.of("X"), "."));
    }

    @Test
    void outputColumns_mustNotBeNullOrEmpty() {
        assertThrows(NullPointerException.class, () ->
                new CsvEnrichment(List.of(List.of("a")), List.of("out"), null, List.of("a"), null, "."));
        assertThrows(IllegalArgumentException.class, () ->
                new CsvEnrichment(List.of(List.of("a")), List.of("out"), null, List.of("a"), List.of(), "."));
    }
}
