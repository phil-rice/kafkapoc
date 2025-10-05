package com.hcltech.rmg.common.errorsor;

import java.text.MessageFormat;
import java.util.*;
import java.util.function.*;

public interface ErrorsOr<T> {

    boolean isError();

    boolean isValue();

    Optional<T> getValue();

    List<String> getErrors();

    ErrorsOr<T> addPrefixIfError(String prefix);

    /**
     * throws an exception if a value
     */
    <T1> ErrorsOr<T1> errorCast();

    // --- Helpers ---
    static <T> ErrorsOr<T> lift(T value) {
        return new Value<>(value);
    }

    static <T> ErrorsOr<T> error(String error) {
        return new Error<>(List.of(error));
    }

    static <T> ErrorsOr<T> error(String pattern, Exception e) {
        return new Error<>(List.of(MessageFormat.format(pattern, e.getClass().getSimpleName(), e.getMessage())));
    }

    static <T> ErrorsOr<T> errors(List<String> errors) {
        return new Error<>(errors);
    }

    default T valueOrThrow() {
        return getValue().orElseThrow(() ->
                new IllegalStateException("Expected value but got errors: " + getErrors()));
    }

    default List<String> errorsOrThrow() {
        if (isError()) return getErrors();
        throw new IllegalStateException("Expected errors but got value: " + getValue().orElse(null));
    }

    default T valueOrDefault(T defaultValue) {
        return getValue().orElse(defaultValue);
    }

    // --- Functional helpers ---
    default <U> ErrorsOr<U> map(Function<? super T, ? extends U> f) {
        return isError() ? ErrorsOr.errors(getErrors()) : ErrorsOr.lift(f.apply(getValue().get()));
    }

    default <U> ErrorsOr<U> flatMap(Function<? super T, ErrorsOr<U>> f) {
        return isError() ? ErrorsOr.errors(getErrors()) : f.apply(getValue().get());
    }

    default ErrorsOr<T> mapError(Function<List<String>, List<String>> f) {
        return isError() ? ErrorsOr.errors(f.apply(getErrors())) : this;
    }
    default ErrorsOr<T> recover(Function<List<String>, T> f) {
        return isError() ? ErrorsOr.lift(f.apply(getErrors())) : this;
    }

    default void ifValue(Consumer<? super T> consumer) {
        if (isValue()) consumer.accept(getValue().get());
    }

    default void ifError(Consumer<? super List<String>> consumer) {
        if (isError()) consumer.accept(getErrors());
    }
}
