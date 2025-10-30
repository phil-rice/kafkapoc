package com.hcltech.rmg.common.errorsor;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class ErrorsOrTest {

    @Nested
    class ConstructionAndPredicates {
        @Test
        void liftCreatesValue() {
            ErrorsOr<Integer> eo = ErrorsOr.lift(42);
            assertTrue(eo.isValue());
            assertFalse(eo.isError());
            assertEquals(Optional.of(42), eo.getValue());
            assertTrue(eo.getErrors().isEmpty());
        }

        @Test
        void errorCreatesError() {
            ErrorsOr<String> eo = ErrorsOr.error("boom");
            assertTrue(eo.isError());
            assertFalse(eo.isValue());
            assertEquals(List.of("boom"), eo.getErrors());
            assertEquals(Optional.empty(), eo.getValue());
        }

        @Test
        void errorsCreatesErrorWithAllMessages() {
            List<String> errs = List.of("a", "b", "c");
            ErrorsOr<Double> eo = ErrorsOr.errors(errs);
            assertTrue(eo.isError());
            assertEquals(errs, eo.getErrors());
        }

        @Test
        void errorsFactoryRejectsEmptyList() {
            assertThrows(IllegalArgumentException.class, () -> ErrorsOr.errors(List.of()));
        }
    }

    @Nested
    class ExtractorsAndDefaults {
        @Test
        void valueOrThrowOnValue() {
            ErrorsOr<String> eo = ErrorsOr.lift("ok");
            assertEquals("ok", eo.valueOrThrow());
        }

        @Test
        void valueOrThrowOnErrorThrows() {
            ErrorsOr<String> eo = ErrorsOr.error("nope");
            IllegalStateException ex = assertThrows(IllegalStateException.class, eo::valueOrThrow);
            assertTrue(ex.getMessage().contains("nope"));
        }

        @Test
        void errorsOrThrowOnError() {
            ErrorsOr<Integer> eo = ErrorsOr.errors(List.of("x", "y"));
            assertEquals(List.of("x", "y"), eo.errorsOrThrow());
        }

        @Test
        void errorsOrThrowOnValueThrows() {
            ErrorsOr<Integer> eo = ErrorsOr.lift(7);
            IllegalStateException ex = assertThrows(IllegalStateException.class, eo::errorsOrThrow);
            assertTrue(ex.getMessage().contains("7"));
        }

        @Test
        void valueOrDefaultReturnsActualValue() {
            ErrorsOr<String> eo = ErrorsOr.lift("present");
            assertEquals("present", eo.valueOrDefault("fallback"));
        }

        @Test
        void valueOrDefaultReturnsFallbackOnError() {
            ErrorsOr<String> eo = ErrorsOr.error("missing");
            assertEquals("fallback", eo.valueOrDefault("fallback"));
        }
    }

    @Nested
    class FunctionalHelpers {
        @Test
        void mapAppliesOnValue() {
            ErrorsOr<Integer> start = ErrorsOr.lift(10);
            ErrorsOr<String> mapped = start.map(n -> "n=" + n);
            assertTrue(mapped.isValue());
            assertEquals("n=10", mapped.getValue().orElseThrow());
        }

        @Test
        void mapPassesThroughError() {
            ErrorsOr<Integer> start = ErrorsOr.error("bad");
            ErrorsOr<String> mapped = start.map(Object::toString);
            assertTrue(mapped.isError());
            assertEquals(List.of("bad"), mapped.getErrors());
        }

        @Test
        void flatMapChainsOnValueToValue() {
            ErrorsOr<Integer> start = ErrorsOr.lift(5);
            ErrorsOr<Integer> out = start.flatMap(n -> ErrorsOr.lift(n * 2));
            assertTrue(out.isValue());
            assertEquals(10, out.getValue().orElseThrow());
        }

        @Test
        void flatMapChainsOnValueToError() {
            ErrorsOr<Integer> start = ErrorsOr.lift(5);
            ErrorsOr<Integer> out = start.flatMap(n -> ErrorsOr.error("oops " + n));
            assertTrue(out.isError());
            assertEquals(List.of("oops 5"), out.getErrors());
        }

        @Test
        void flatMapShortCircuitsOnError() {
            ErrorsOr<Integer> start = ErrorsOr.error("first");
            ErrorsOr<Integer> out = start.flatMap(n -> ErrorsOr.lift(n * 99));
            assertTrue(out.isError());
            assertEquals(List.of("first"), out.getErrors());
        }

        @Test
        void mapErrorTransformsErrorsOnly() {
            ErrorsOr<Integer> bad = ErrorsOr.errors(List.of("a", "b"));
            ErrorsOr<Integer> transformed = bad.mapError(errs -> errs.stream().map(e -> "E:" + e).toList());
            assertTrue(transformed.isError());
            assertEquals(List.of("E:a", "E:b"), transformed.getErrors());

            ErrorsOr<Integer> good = ErrorsOr.lift(3);
            ErrorsOr<Integer> unchanged = good.mapError(errs -> List.of("should not happen"));
            assertTrue(unchanged.isValue());
            assertEquals(3, unchanged.getValue().orElseThrow());
        }

        @Test
        void ifValueAndIfErrorInvokeConsumersConditionally() {
            ErrorsOr<String> good = ErrorsOr.lift("yay");
            ErrorsOr<String> bad = ErrorsOr.error("nay");

            var valueSink = new java.util.concurrent.atomic.AtomicReference<>("");
            var errorSink = new java.util.concurrent.atomic.AtomicReference<List<String>>(List.of());

            good.ifValue(valueSink::set);
            bad.ifError(errorSink::set);

            assertEquals("yay", valueSink.get());
            assertEquals(List.of("nay"), errorSink.get());
        }

        @Test
        void addPrefixIfError_prefixesEachError_andNoOpOnValue() {
            ErrorsOr<Object> err = ErrorsOr.errors(List.of("e1", "e2"))
                    .addPrefixIfError("ctx: ");

            assertTrue(err.isError());
            assertEquals(List.of("ctx: e1", "ctx: e2"), err.getErrors());

            ErrorsOr<Integer> val = ErrorsOr.lift(99).addPrefixIfError("ctx: ");
            assertTrue(val.isValue());
            assertEquals(99, val.getValue().orElseThrow());
        }

        @Test
        void fold_returnsValue_onValue() {
            ErrorsOr<String> val = ErrorsOr.lift("ok");
            String out = val.foldError(errs -> "fallback:" + String.join(",", errs));
            assertEquals("ok", out, "fold must return the value when isValue()");
        }

        @Test
        void fold_appliesOnError_andReturnsFallback() {
            ErrorsOr<String> err = ErrorsOr.errors(List.of("a", "b"));
            String out = err.foldError(errs -> "fallback:" + String.join("|", errs));
            assertEquals("fallback:a|b", out);
        }

        @Test
        void errorPatternWithException_formatsNicely() {
            Exception ex = new IllegalArgumentException("bad arg");
            ErrorsOr<Void> eo = ErrorsOr.error("Failed op: {0}: {1}", ex);
            assertTrue(eo.isError());
            String msg = eo.getErrors().get(0);
            assertTrue(msg.contains("IllegalArgumentException"));
            assertTrue(msg.contains("bad arg"));
        }
    }

    @Nested
    class ConvenienceCombinatorsExamples {
        // If you later add ErrorsOr.sequence / traverse, here are ready-to-go tests.

        @Test
        void sequenceAllSuccess() {
            List<ErrorsOr<Integer>> in = List.of(ErrorsOr.lift(1), ErrorsOr.lift(2), ErrorsOr.lift(3));
            ErrorsOr<List<Integer>> out = sequence(in);
            assertTrue(out.isValue());
            assertEquals(List.of(1, 2, 3), out.getValue().orElseThrow());
        }

        @Test
        void sequenceAggregatesFirstErrorSet() {
            List<ErrorsOr<Integer>> in = List.of(
                    ErrorsOr.lift(1),
                    ErrorsOr.errors(List.of("e1", "e2")),
                    ErrorsOr.error("e3")
            );
            ErrorsOr<List<Integer>> out = sequence(in);
            assertTrue(out.isError());
            assertEquals(List.of("e1", "e2", "e3"), out.getErrors());
        }

        // Minimal sequence utility for testing purpose; consider moving into your API.
        private static <T> ErrorsOr<List<T>> sequence(List<ErrorsOr<T>> items) {
            List<String> allErrors = new ArrayList<>();
            List<T> values = new ArrayList<>();
            for (ErrorsOr<T> eo : items) {
                if (eo.isError()) {
                    allErrors.addAll(eo.getErrors());
                } else {
                    values.add(eo.getValue().orElseThrow());
                }
            }
            return allErrors.isEmpty() ? ErrorsOr.lift(List.copyOf(values)) : ErrorsOr.errors(List.copyOf(allErrors));
        }
    }

    @Nested
    class ThrowingHelpersAndTryWrappers {

        @Test
        void tryingSupplier_success() {
            ErrorsOr<String> eo = ErrorsOr.trying(() -> "ok");
            assertEquals("ok", eo.valueOrThrow());
        }

        @Test
        void tryingSupplier_failure() {
            RuntimeException boom = new RuntimeException("kaput");
            ErrorsOr<Integer> eo = ErrorsOr.trying(() -> {
                throw boom;
            });
            var errs = eo.errorsOrThrow();
            assertTrue(String.join(" | ", errs).toLowerCase().contains("kaput"));
        }

        @Test
        void tryingSupplier_withCustomFormatter() {
            ErrorsOr<Integer> eo = ErrorsOr.trying(
                    () -> {
                        throw new IllegalStateException("bad");
                    },
                    ex -> "CUSTOM: " + ex.getClass().getSimpleName() + " / " + ex.getMessage()
            );
            var errs = eo.errorsOrThrow();
            String all = String.join(" | ", errs);
            assertTrue(all.contains("CUSTOM: IllegalStateException"));
            assertTrue(all.contains("bad"));
        }


        @Test
        void mapTry_onValue_success() {
            ErrorsOr<Integer> start = ErrorsOr.lift(10);
            ErrorsOr<String> out = start.mapTry(n -> "n=" + (n * 2));
            assertEquals("n=20", out.valueOrThrow());
        }

        @Test
        void mapTry_onValue_throws_becomesError() {
            ErrorsOr<Integer> start = ErrorsOr.lift(10);
            ErrorsOr<String> out = start.mapTry(n -> {
                throw new RuntimeException("boom " + n);
            });
            var errs = out.errorsOrThrow();
            String all = String.join(" | ", errs).toLowerCase();
            assertTrue(all.contains("boom 10"));
        }

        @Test
        void mapTry_onError_shortCircuits() {
            ErrorsOr<Integer> start = ErrorsOr.error("bad");
            ErrorsOr<String> out = start.mapTry(n -> "n=" + n);
            assertEquals(List.of("bad"), out.errorsOrThrow());
        }

        @Test
        void flatMapTry_onValue_success_value() {
            ErrorsOr<Integer> start = ErrorsOr.lift(3);
            ErrorsOr<Integer> out = start.flatMapTry(n -> ErrorsOr.lift(n * 7));
            assertEquals(21, out.valueOrThrow());
        }

        @Test
        void flatMapTry_onValue_throws_becomesError() {
            ErrorsOr<Integer> start = ErrorsOr.lift(3);
            ErrorsOr<Integer> out = start.flatMapTry(n -> {
                throw new IllegalArgumentException("x" + n);
            });
            var errs = out.errorsOrThrow();
            String all = String.join(" | ", errs).toLowerCase();
            assertTrue(all.contains("illegalargumentexception"));
            assertTrue(all.contains("x3"));
        }

        @Test
        void flatMapTry_onError_shortCircuits() {
            ErrorsOr<Integer> start = ErrorsOr.errors(List.of("e1", "e2"));
            ErrorsOr<Integer> out = start.flatMapTry(n -> ErrorsOr.lift(n + 1));
            assertEquals(List.of("e1", "e2"), out.errorsOrThrow());
        }
    }

    @Test
    void recover_applies_function_on_error_and_returns_value() {
        ErrorsOr<String> err = ErrorsOr.errors(List.of("e1", "e2"));
        ErrorsOr<String> recovered = err.recover(es -> "fixed:" + String.join("|", es));
        assertTrue(recovered.isValue());
        assertEquals("fixed:e1|e2", recovered.valueOrThrow());
    }

    @Test
    void recover_on_value_is_noop_and_does_not_invoke_function() {
        ErrorsOr<Integer> val = ErrorsOr.lift(123);
        java.util.concurrent.atomic.AtomicBoolean called = new java.util.concurrent.atomic.AtomicBoolean(false);

        ErrorsOr<Integer> same = val.recover(es -> {
            called.set(true);
            return -1; // should not run
        });

        assertSame(val, same, "recover() on value should return the same instance");
        assertFalse(called.get(), "recover() must not invoke the function when already a value");
        assertEquals(123, same.valueOrThrow());
    }

    @Test
    void trying_with_custom_formatter_success_path_does_not_call_formatter() {
        java.util.concurrent.atomic.AtomicBoolean formatterCalled = new java.util.concurrent.atomic.AtomicBoolean(false);

        ErrorsOr<String> ok = ErrorsOr.trying(
                () -> "ok",
                ex -> {
                    formatterCalled.set(true);
                    return "SHOULD_NOT_BE_USED";
                }
        );

        assertTrue(ok.isValue());
        assertEquals("ok", ok.valueOrThrow());
        assertFalse(formatterCalled.get(), "Formatter must not be called on success");
    }

}
