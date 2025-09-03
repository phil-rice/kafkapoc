package com.hcltech.rmg.interfaces.outcome;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public sealed interface Outcome<T>
        permits Value, Errors, Retry {

    // ---- Constructors / factories -------------------------------------------------------------

    static <T> Outcome<T> lift(T value) { return value(value); }        // alias
    static <T> Outcome<T> value(T value) { return new Value<>(value); }

    static <T> Outcome<T> error(String error) { return new Errors<>(List.of(error)); }
    static <T> Outcome<T> errors(List<String> errors) { return new Errors<>(errors); }

    static <T> Outcome<T> retry(RetrySpec spec) { return new Retry<>(spec); }

    // ---- Predicates ---------------------------------------------------------------------------

    default boolean isValue()  { return this instanceof Value<?>; }
    default boolean isErrors() { return this instanceof Errors<?>; }
    default boolean isRetry()  { return this instanceof Retry<?>; }

    // ---- Extractors / defaults ----------------------------------------------------------------

    default Optional<T> valueOpt() {
        if (this instanceof Value<?> v) {
            @SuppressWarnings("unchecked") T t = (T) v.value();
            return Optional.of(t);
        }
        return Optional.empty();
    }

    default T valueOrThrow() {
        if (this instanceof Value<?> v) {
            @SuppressWarnings("unchecked") T t = (T) v.value();
            return t;
        }
        throw new IllegalStateException("Expected value but was: " + this);
    }

    default T valueOrDefault(T defaultValue) {
        return valueOpt().orElse(defaultValue);
    }

    default T valueOrElseGet(Supplier<? extends T> supplier) {
        return valueOpt().orElseGet(supplier);
    }

    /** Returns the error list or throws if not an Errors. */
    default List<String> errorOrThrow() {
        if (this instanceof Errors<?> e) {
            return e.errors();
        }
        throw new IllegalStateException("Expected errors but was: " + this);
    }

    /** Alias if you prefer plural naming. */
    default List<String> errorsOrThrow() { return errorOrThrow(); }

    /** Returns the retry spec or throws if not a Retry. */
    default RetrySpec retryOrThrow() {
        if (this instanceof Retry<?> r) {
            return r.spec();
        }
        throw new IllegalStateException("Expected retry but was: " + this);
    }

    // ---- Transformations ----------------------------------------------------------------------

    default <U> Outcome<U> map(Function<? super T, ? extends U> f) {
        if (this instanceof Value<?> v) {
            @SuppressWarnings("unchecked") T t = (T) v.value();
            return Outcome.value(f.apply(t));
        } else if (this instanceof Errors<?> e) {
            @SuppressWarnings("unchecked") List<String> errs = (List<String>) e.errors();
            return Outcome.errors(errs);
        } else {
            RetrySpec spec = ((Retry<?>) this).spec();
            return Outcome.retry(spec);
        }
    }

    default <U> Outcome<U> flatMap(Function<? super T, Outcome<U>> f) {
        if (this instanceof Value<?> v) {
            @SuppressWarnings("unchecked") T t = (T) v.value();
            return f.apply(t);
        } else if (this instanceof Errors<?> e) {
            @SuppressWarnings("unchecked") List<String> errs = (List<String>) e.errors();
            return Outcome.errors(errs);
        } else {
            RetrySpec spec = ((Retry<?>) this).spec();
            return Outcome.retry(spec);
        }
    }

    /** Total handler for all cases. */
    default <R> R fold(Function<? super T, ? extends R> onValue,
                       Function<? super List<String>, ? extends R> onErrors,
                       Function<? super RetrySpec, ? extends R> onRetry) {
        if (this instanceof Value<?> v) {
            @SuppressWarnings("unchecked") T t = (T) v.value();
            return onValue.apply(t);
        } else if (this instanceof Errors<?> e) {
            return onErrors.apply(e.errors());
        } else {
            return onRetry.apply(((Retry<?>) this).spec());
        }
    }


}
