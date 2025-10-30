package com.hcltech.rmg.common.errorsor;

import com.hcltech.rmg.common.function.ThrowingFunction;
import com.hcltech.rmg.common.function.ThrowingRunnable;
import com.hcltech.rmg.common.function.ThrowingSupplier;

import java.text.MessageFormat;
import java.util.*;
import java.util.function.*;

public interface ErrorsOr<T> {

    boolean isError();

    boolean isValue();

    Optional<T> getValue();

    List<String> getErrors();

    ErrorsOr<T> addPrefixIfError(String prefix);

    T foldError(Function<List<String>, T> onError);

    <T1> T1 fold(Function<T, T1> onValue, Function<List<String>, T1> onError);

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

    /** Wrap a throwing supplier -> ErrorsOr. */
    static <T> ErrorsOr<T> trying(ThrowingSupplier<T> body) {
        try {
            return ErrorsOr.lift(body.get());
        } catch (Exception e) {
            return ErrorsOr.error("Evaluation error: {0}: {1}", e);
        }
    }

    /** Wrap a throwing supplier with a custom formatter (no alloc if happy path). */
    static <T> ErrorsOr<T> trying(ThrowingSupplier<T> body, java.util.function.Function<Exception,String> toMsg) {
        try {
            return ErrorsOr.lift(body.get());
        } catch (Exception e) {
            return ErrorsOr.error(toMsg.apply(e));
        }
    }


    /** Map + try: apply f to value (which may throw). */
    default <U> ErrorsOr<U> mapTry(ThrowingFunction<? super T, ? extends U> f) {
        if (isError()) return ErrorsOr.errors(getErrors());
        try {
            return ErrorsOr.lift(f.apply(getValue().get()));
        } catch (Exception e) {
            return ErrorsOr.error("Evaluation error: {0}: {1}", e);
        }
    }

    /** FlatMap + try: apply f returning ErrorsOr, where f may throw. */
    default <U> ErrorsOr<U> flatMapTry(ThrowingFunction<? super T, ErrorsOr<U>> f) {
        if (isError()) return ErrorsOr.errors(getErrors());
        try {
            return f.apply(getValue().get());
        } catch (Exception e) {
            return ErrorsOr.error("Evaluation error: {0}: {1}", e);
        }
    }
}
