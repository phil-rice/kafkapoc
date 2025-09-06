package com.hcltech.rmg.interfaces.repository;

import com.hcltech.rmg.interfaces.pipeline.IOneToManyPipeline;
import com.hcltech.rmg.interfaces.pipeline.IOneToOnePipeline;
import com.hcltech.rmg.interfaces.pipeline.ValueTC;
import com.hcltech.rmg.interfaces.retry.RetryPolicyConfig;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JUnit 5 tests for PipelineBuilder / PipelineStageBuilder with timeout support.
 * Focus: compile-time typing, stage ordering, overload coverage, and timeout defaults/overrides.
 */
public class PipelineStageBuilderTest {

    @SuppressWarnings("unchecked")
    private static RetryPolicyConfig dummyRetry() {
        return Mockito.mock(RetryPolicyConfig.class);
    }

    @SuppressWarnings("unchecked")
    private static <F> ValueTC<F> dummyValueTC() {
        return (ValueTC<F>) Mockito.mock(ValueTC.class);
    }

    private static final BiFunction<String, Throwable, String> DEFAULT_ERR = (stage, t) -> "err@" + stage;

    // Home-built one-to-one pipeline "mock". We only care about generics, never run it.
    private static <F, T> IOneToOnePipeline<F, T> oneToOne(Class<F> from, Class<T> to) {
        return f -> null; // CompletionStage<Outcome<T>> (not executed in these tests)
    }

    // Home-built one-to-many pipeline "mock". We only care about generics, never run it.
    private static <F, T> IOneToManyPipeline<F, T> oneToMany(Class<F> from, Class<T> to) {
        return ( f) -> null; // CompletionStage<Outcome<List<T>>> (not executed)
    }

    // Simple marker types to clarify generic transitions
    static final class A {}
    static final class B {}
    static final class C {}
    static final class D {}
    static final class E {}
    static final class F {}

    // ----- Tests -----

    @Test
    void chain_three_one_to_one_uses_default_timeout_and_builds() {
        long defaultTimeoutMs = 30_000L;
        var builder = new PipelineBuilder<String>(dummyValueTC(), DEFAULT_ERR, dummyRetry(), defaultTimeoutMs);

        PipelineDetails<String, C> details = builder
                .oneToOne("s1", String.class, oneToOne(String.class, A.class))
                .oneToOne("s2", A.class,      oneToOne(A.class, B.class))
                .oneToOne("s3", B.class,      oneToOne(B.class, C.class))
                .build();

        assertNotNull(details);
        assertEquals(3, details.stages().size());
        assertOrder(details.stages(), "s1", "s2", "s3");

        // All should inherit default timeout
        assertEquals(defaultTimeoutMs, timeoutOf(details, "s1"));
        assertEquals(defaultTimeoutMs, timeoutOf(details, "s2"));
        assertEquals(defaultTimeoutMs, timeoutOf(details, "s3"));
    }

    @Test
    void chain_three_one_to_many_uses_default_timeout_and_builds() {
        long defaultTimeoutMs = 45_000L;
        var builder = new PipelineBuilder<Integer>(dummyValueTC(), DEFAULT_ERR, dummyRetry(), defaultTimeoutMs);

        PipelineDetails<Integer, F> details = builder
                .oneToMany("m1", Integer.class, oneToMany(Integer.class, D.class))
                .oneToMany("m2", D.class,       oneToMany(D.class, E.class))
                .oneToMany("m3", E.class,       oneToMany(E.class, F.class))
                .build();

        assertNotNull(details);
        assertEquals(3, details.stages().size());
        assertOrder(details.stages(), "m1", "m2", "m3");

        // All should inherit default timeout
        assertEquals(defaultTimeoutMs, timeoutOf(details, "m1"));
        assertEquals(defaultTimeoutMs, timeoutOf(details, "m2"));
        assertEquals(defaultTimeoutMs, timeoutOf(details, "m3"));
    }

    @Test
    void mixed_chain_with_all_overrides_including_timeouts_builds_and_preserves_order() {
        long defaultTimeoutMs = 10_000L;
        var builder = new PipelineBuilder<Long>(dummyValueTC(), DEFAULT_ERR, dummyRetry(), defaultTimeoutMs);

        long tOnly1  = 5_000L;
        long tOnly2  = 60_000L;
        long tBoth   = 90_000L;

        BiFunction<String, Throwable, String> customErr1 = (stage, t) -> "custom1@" + stage;
        BiFunction<String, Throwable, String> customErr2 = (stage, t) -> "custom2@" + stage;

        RetryPolicyConfig r1 = dummyRetry();
        RetryPolicyConfig r2 = dummyRetry();

        PipelineDetails<Long, F> details = builder
                // 1) Long -> A (one-to-one, all defaults)
                .oneToOne("x1", Long.class, oneToOne(Long.class, A.class))
                // 2) A -> B (one-to-one with retry override)
                .oneToOne("x2", A.class,    oneToOne(A.class, B.class), r1)
                // 3) B -> C (one-to-one with error override)
                .oneToOne("x3", B.class,    oneToOne(B.class, C.class), customErr1)
                // 4) C -> D (one-to-one with timeout-only override)
                .oneToOne("x4", C.class,    oneToOne(C.class, D.class), tOnly1)
                // 5) D -> E (one-to-one with retry+error overrides; default timeout)
                .oneToOne("x5", D.class,    oneToOne(D.class, E.class), r2, customErr2)
                // 6) E -> F (one-to-one with retry+error+timeout override)
                .oneToOne("x6", E.class,    oneToOne(E.class, F.class), r2, customErr2, tBoth)

                // 7) F -> A (one-to-many, defaults)
                .oneToMany("y1", F.class,   oneToMany(F.class, A.class))
                // 8) A -> B (one-to-many with retry override)
                .oneToMany("y2", A.class,   oneToMany(A.class, B.class), r1)
                // 9) B -> C (one-to-many with error override)
                .oneToMany("y3", B.class,   oneToMany(B.class, C.class), customErr1)
                // 10) C -> D (one-to-many with timeout-only override)
                .oneToMany("y4", C.class,   oneToMany(C.class, D.class), tOnly2)
                // 11) D -> E (one-to-many with retry+error overrides; default timeout)
                .oneToMany("y5", D.class,   oneToMany(D.class, E.class), r2, customErr2)
                // 12) E -> F (one-to-many with retry+error+timeout override; Long nullable accepted)
                .oneToMany("y6", E.class,   oneToMany(E.class, F.class), r2, customErr2, tBoth)
                .build();

        assertNotNull(details);
        assertEquals(12, details.stages().size());
        assertOrder(details.stages(), "x1","x2","x3","x4","x5","x6","y1","y2","y3","y4","y5","y6");

        // Defaults
        assertEquals(defaultTimeoutMs, timeoutOf(details, "x1"));
        assertEquals(defaultTimeoutMs, timeoutOf(details, "x2"));
        assertEquals(defaultTimeoutMs, timeoutOf(details, "x3"));
        assertEquals(defaultTimeoutMs, timeoutOf(details, "y1"));
        assertEquals(defaultTimeoutMs, timeoutOf(details, "y2"));
        assertEquals(defaultTimeoutMs, timeoutOf(details, "y3"));
        assertEquals(defaultTimeoutMs, timeoutOf(details, "y5"));

        // Timeout-only overrides
        assertEquals(tOnly1, timeoutOf(details, "x4"));
        assertEquals(tOnly2, timeoutOf(details, "y4"));

        // Retry+error+timeout overrides
        assertEquals(tBoth,  timeoutOf(details, "x6"));
        assertEquals(tBoth,  timeoutOf(details, "y6"));
    }

    @Test
    void retry_error_timeout_nullable_combo_falls_back_to_default_timeout() {
        long defaultTimeoutMs = 12_345L;
        var builder = new PipelineBuilder<String>(dummyValueTC(), DEFAULT_ERR, dummyRetry(), defaultTimeoutMs);

        PipelineDetails<String, B> details = builder
                .oneToOne("p1", String.class, oneToOne(String.class, A.class))
                // pass null timeout on the 3-arg override: should fallback to defaultTimeoutMs
                .oneToOne("p2", A.class, oneToOne(A.class, B.class), dummyRetry(), (stage,t) -> "oops@" + stage, null)
                .build();

        assertNotNull(details);
        assertEquals(2, details.stages().size());
        assertOrder(details.stages(), "p1", "p2");
        assertEquals(defaultTimeoutMs, timeoutOf(details, "p1"));
        assertEquals(defaultTimeoutMs, timeoutOf(details, "p2"));
    }

    @Test
    void duplicate_stage_name_throws() {
        long defaultTimeoutMs = 1_000L;
        var builder = new PipelineBuilder<String>(dummyValueTC(), DEFAULT_ERR, dummyRetry(), defaultTimeoutMs);

        assertThrows(IllegalArgumentException.class, () ->
                builder
                        .oneToOne("dup", String.class, oneToOne(String.class, A.class))
                        .oneToOne("dup", A.class,      oneToOne(A.class, B.class)) // duplicate name should throw
                        .build()
        );
    }

    // ----- Helpers -----

    private static void assertOrder(LinkedHashMap<String, PipelineStageDetails<?, ?>> map, String... expectedKeys) {
        assertEquals(expectedKeys.length, map.size(), "stage count mismatch");
        int i = 0;
        for (String key : map.keySet()) {
            assertEquals(expectedKeys[i++], key, "insertion order mismatch at index " + (i - 1));
        }
    }

    private static long timeoutOf(PipelineDetails<?, ?> details, String stage) {
        PipelineStageDetails<?, ?> psd = details.stages().get(stage);
        assertNotNull(psd, "Missing stage: " + stage);
        return psd.timeOutMs();
    }
}
