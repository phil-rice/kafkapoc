// src/test/java/com/hcltech/rmg/interfaces/outcome/OutcomeTest.java
package com.hcltech.rmg.interfaces.outcome;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class OutcomeTest {

    @Nested
    class ConstructorsAndPredicates {
        @Test
        void liftAndValueCreateValue() {
            Outcome<Integer> a = Outcome.lift(42);
            Outcome<Integer> b = Outcome.value(42);

            assertTrue(a.isValue());
            assertFalse(a.isErrors());
            assertFalse(a.isRetry());
            assertEquals(Optional.of(42), a.valueOpt());

            // assuming Value is a record (value-based equality)
            assertEquals(a, b);
        }

        @Test
        void errorAndErrorsCreateErrors() {
            Outcome<String> one = Outcome.error("boom");
            assertTrue(one.isErrors());
            assertEquals(List.of("boom"), one.errorOrThrow());

            Outcome<String> many = Outcome.errors(List.of("a", "b"));
            assertTrue(many.isErrors());
            assertEquals(List.of("a", "b"), many.errorsOrThrow());
        }

        @Test
        void errorsMustNotBeEmpty() {
            assertThrows(IllegalArgumentException.class, () -> Outcome.errors(List.of()));
        }

        @Test
        void valueMustNotBeNull() {
            assertThrows(NullPointerException.class, () -> Outcome.value(null));
        }

        @Test
        void retryCreatedWithSpec() {
            RetrySpec spec = new RetrySpec("transient");
            Outcome<String> r = Outcome.retry(spec);
            assertTrue(r.isRetry());
            assertEquals(spec, r.retryOrThrow());
            assertEquals("transient", r.retryOrThrow().reason());
        }

        @Test
        void errorsListIsImmutableCopy() {
            List<String> base = new ArrayList<>(List.of("x"));
            Outcome<String> e = Outcome.errors(base);
            base.add("y"); // mutate original
            assertEquals(List.of("x"), e.errorOrThrow()); // copy held by Outcome
            assertThrows(UnsupportedOperationException.class, () -> e.errorOrThrow().add("z"));
        }
    }

    @Nested
    class ExtractorsAndDefaults {
        @Test
        void valueOrThrowOnValue() {
            Outcome<String> v = Outcome.value("ok");
            assertEquals("ok", v.valueOrThrow());
        }

        @Test
        void valueOrThrowOnNonValueThrows() {
            Outcome<String> e = Outcome.error("nope");
            assertThrows(IllegalStateException.class, e::valueOrThrow);

            Outcome<String> r = Outcome.retry(new RetrySpec("try"));
            assertThrows(IllegalStateException.class, r::valueOrThrow);
        }

        @Test
        void valueOrDefault() {
            Outcome<String> v = Outcome.value("present");
            Outcome<String> e = Outcome.error("missing");
            assertEquals("present", v.valueOrDefault("fallback"));
            assertEquals("fallback", e.valueOrDefault("fallback"));
        }

        @Test
        void errorOrThrowAndRetryOrThrow() {
            Outcome<Integer> e = Outcome.errors(List.of("a", "b"));
            assertEquals(List.of("a", "b"), e.errorOrThrow());
            assertThrows(IllegalStateException.class, e::retryOrThrow);

            Outcome<Integer> r = Outcome.retry(new RetrySpec("x"));
            assertEquals("x", r.retryOrThrow().reason());
            assertThrows(IllegalStateException.class, r::errorOrThrow);
        }
    }

    @Nested
    class MapAndFlatMap {
        @Test
        void mapAppliesOnlyOnValue() {
            AtomicInteger calls = new AtomicInteger(0);
            Function<Integer, String> f = n -> { calls.incrementAndGet(); return "n=" + n; };

            Outcome<String> mv = Outcome.<Integer>value(7).map(f);
            assertEquals("n=7", mv.valueOrThrow());
            assertEquals(1, calls.get());

            Outcome<String> me = Outcome.<Integer>errors(List.of("err")).map(f);
            assertTrue(me.isErrors());
            assertEquals(1, calls.get()); // not invoked

            Outcome<String> mr = Outcome.<Integer>retry(new RetrySpec("try")).map(f);
            assertTrue(mr.isRetry());
            assertEquals(1, calls.get()); // still not invoked
        }

        @Test
        void flatMapChainsAndShortCircuits() {
            Function<Outcome<Integer>, Outcome<String>> toStr =
                    (Outcome<Integer> v) -> v.flatMap(n -> Outcome.value("v=" + n));

            assertEquals("v=5", toStr.apply(Outcome.value(5)).valueOrThrow());

            Outcome<String> e = Outcome.<Integer>errors(List.of("bad")).flatMap(n -> Outcome.value("x"));
            assertTrue(e.isErrors());
            assertEquals(List.of("bad"), e.errorOrThrow());

            Outcome<String> r = Outcome.<Integer>retry(new RetrySpec("r"))
                    .flatMap(n -> Outcome.value("x"));
            assertTrue(r.isRetry());
        }

        @Test
        void flatMapCanReturnErrorsOrRetry() {
            Outcome<String> fromValueToError =
                    Outcome.value(1).flatMap(n -> Outcome.errors(List.of("boom")));
            assertTrue(fromValueToError.isErrors());
            assertEquals(List.of("boom"), fromValueToError.errorOrThrow());

            Outcome<String> fromValueToRetry =
                    Outcome.value(1).flatMap(n -> Outcome.retry(new RetrySpec("transient")));
            assertTrue(fromValueToRetry.isRetry());
        }
    }

    @Nested
    class Fold {
        @Test
        void foldHandlesAllBranches() {
            var v = Outcome.value(10).fold(
                    x -> "value:" + x,
                    errs -> "errors:" + errs.size(),
                    spec -> "retry:" + spec.reason()
            );
            assertEquals("value:10", v);

            var e = Outcome.errors(List.of("a", "b")).fold(
                    x -> "value:" + x,
                    errs -> "errors:" + errs.size(),
                    spec -> "retry:" + spec.reason()
            );
            assertEquals("errors:2", e);

            var r = Outcome.retry(new RetrySpec("r")).fold(
                    x -> "value:" + x,
                    errs -> "errors:" + errs.size(),
                    spec -> "retry:" + spec.reason()
            );
            assertEquals("retry:r", r);
        }
    }

    // If you also added a convenience factory: Outcome.retry(String)
    // @Test
    // void retryConvenienceFactory() {
    //     Outcome<String> r = Outcome.retry("RATE_LIMITED");
    //     assertEquals("RATE_LIMITED", r.retryOrThrow().reason());
    // }
}
