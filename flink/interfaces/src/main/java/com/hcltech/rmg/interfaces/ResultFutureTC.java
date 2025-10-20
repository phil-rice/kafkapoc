package com.hcltech.rmg.interfaces;

/**
 * A "typeclass" (capability) that knows how to complete a result of type {@code ResultFuture}
 * with a value {@code Out} or with an error.
 *
 * <p>Why this exists:
 * Instead of binding your business logic to a specific future/callback type (e.g. Flink's
 * {@code ResultFuture}, Java's {@code CompletableFuture}, Reactor {@code MonoSink}, etc.),
 * you abstract over "how to complete" it. The generic {@code ResultFuture} is just an opaque
 * handle; this typeclass supplies the two operations you need to finish it.</p>
 *
 * <p>Zero-cost: the JVM will inline these tiny methods; there are no per-call wrapper objects
 * if you pass the existing future/sink instance through.</p>
 *
 * @param <ResultFuture>  the concrete future/sink type you use at call sites
 * @param <Out>           the produced value type when the async work succeeds
 */
public interface ResultFutureTC<ResultFuture, Out> {

    /**
     * Complete {@code resultFuture} successfully with {@code out}.
     */
    void complete(ResultFuture resultFuture, Out out);

    /**
     * Complete {@code resultFuture} exceptionally with {@code error}.
     */
    void completeExceptionally(ResultFuture resultFuture, Throwable error);
}
