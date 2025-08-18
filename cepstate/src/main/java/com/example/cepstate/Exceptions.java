package com.example.cepstate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

public final class Exceptions {
    private Exceptions() {}

    @FunctionalInterface
    public interface CheckedSupplier<T> { T get() throws Exception; }

    /** Turn a try/catch around a stage-producing call into a single expression. */
    public static <T> CompletionStage<T> wrap(CheckedSupplier<? extends CompletionStage<T>> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /** Attach a side-effect on failure and rethrow to keep the chain exceptional. */
    public static <T> CompletionStage<T> sideEffectOnFailure(CompletionStage<T> stage, Consumer<Throwable> onFailure) {
        return stage.exceptionally(ex -> {
            onFailure.accept(unwrap(ex));
            throw propagate(ex);
        });
    }

    /** Unwrap nested CompletionException if present. */
    public static Throwable unwrap(Throwable ex) {
        return (ex instanceof CompletionException ce && ce.getCause() != null) ? ce.getCause() : ex;
    }

    /** Rethrow preserving RuntimeException, wrap others in CompletionException. */
    public static RuntimeException propagate(Throwable ex) {
        if (ex instanceof RuntimeException re) return re;
        return new CompletionException(ex);
    }
}
