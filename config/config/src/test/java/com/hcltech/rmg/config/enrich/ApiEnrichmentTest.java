package com.hcltech.rmg.config.enrich;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ApiEnrichmentTest {

    private static ApiEnrichment valid() {
        return new ApiEnrichment(
                List.of(List.of("a"), List.of("b")),
                List.of("X"),
                "https://api.example.com/v1/lookup",
                "q",
                500,
                1500
        );
    }

    @Test
    void validConfig_constructs() {
        assertDoesNotThrow(ApiEnrichmentTest::valid);
    }

    @Test
    void asDependencies_returnsSelf() {
        var cfg = valid();
        var deps = cfg.asDependencies();
        assertEquals(1, deps.size());
        assertSame(cfg, deps.get(0));
    }

    @Nested
    class InputsValidation {

        @Test
        void nullInputs_rejected() {
            NullPointerException ex = assertThrows(
                    NullPointerException.class,
                    () -> new ApiEnrichment(null, List.of("X"), "https://x", "q", 1, 1)
            );
            assertTrue(ex.getMessage().toLowerCase().contains("inputs"));
        }

        @Test
        void emptyInputs_rejected() {
            IllegalArgumentException ex = assertThrows(
                    IllegalArgumentException.class,
                    () -> new ApiEnrichment(List.of(), List.of("X"), "https://x", "q", 1, 1)
            );
            assertTrue(ex.getMessage().toLowerCase().contains("inputs"));
        }
    }

    @Nested
    class OutputValidation {

        @Test
        void nullOutput_rejected() {
            NullPointerException ex = assertThrows(
                    NullPointerException.class,
                    () -> new ApiEnrichment(List.of(List.of("a")), null, "https://x", "q", 1, 1)
            );
            assertTrue(ex.getMessage().toLowerCase().contains("output"));
        }

        @Test
        void emptyOutput_rejected() {
            IllegalArgumentException ex = assertThrows(
                    IllegalArgumentException.class,
                    () -> new ApiEnrichment(List.of(List.of("a")), List.of(), "https://x", "q", 1, 1)
            );
            assertTrue(ex.getMessage().toLowerCase().contains("output"));
        }
    }

    @Nested
    class UrlValidation {

        @Test
        void nullUrl_rejected() {
            NullPointerException ex = assertThrows(
                    NullPointerException.class,
                    () -> new ApiEnrichment(List.of(List.of("a")), List.of("X"), null, "q", 1, 1)
            );
            assertTrue(ex.getMessage().toLowerCase().contains("url"));
        }

        @Test
        void blankUrl_rejected() {
            IllegalArgumentException ex = assertThrows(
                    IllegalArgumentException.class,
                    () -> new ApiEnrichment(List.of(List.of("a")), List.of("X"), "  ", "q", 1, 1)
            );
            assertTrue(ex.getMessage().toLowerCase().contains("url"));
        }
    }

    @Nested
    class ParamNameValidation {

        @Test
        void nullParamName_rejected() {
            NullPointerException ex = assertThrows(
                    NullPointerException.class,
                    () -> new ApiEnrichment(List.of(List.of("a")), List.of("X"), "https://x", null, 1, 1)
            );
            assertTrue(ex.getMessage().toLowerCase().contains("paramname"));
        }

        @Test
        void blankParamName_rejected() {
            IllegalArgumentException ex = assertThrows(
                    IllegalArgumentException.class,
                    () -> new ApiEnrichment(List.of(List.of("a")), List.of("X"), "https://x", "  ", 1, 1)
            );
            assertTrue(ex.getMessage().toLowerCase().contains("paramname"));
        }
    }

    @Nested
    class TimeoutValidation {

        @Test
        void nonPositiveConnectTimeout_rejected() {
            IllegalArgumentException ex = assertThrows(
                    IllegalArgumentException.class,
                    () -> new ApiEnrichment(List.of(List.of("a")), List.of("X"), "https://x", "q", 0, 1)
            );
            assertTrue(ex.getMessage().toLowerCase().contains("connecttimeoutms"));
        }

        @Test
        void nonPositiveReadTimeout_rejected() {
            IllegalArgumentException ex = assertThrows(
                    IllegalArgumentException.class,
                    () -> new ApiEnrichment(List.of(List.of("a")), List.of("X"), "https://x", "q", 1, 0)
            );
            assertTrue(ex.getMessage().toLowerCase().contains("timeoutms"));
        }
    }
}
