package com.hcltech.rmg.common.async;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Minimal single-threaded lane API for the operator thread.
 */
public interface ILane<T> {

    // ---- Mutations

    /**
     * Enqueue at tail. Caller must ensure !isFull().
     */
    void enqueue(T t, String correlationId, long startedAtNanos);

    /**
     * Pop head if present; apply callback with FR, T and an external context.
     */
    <C> boolean popHead(C ctx, BiConsumer<T, C> consumer);


    /**
     * Pop head if present, clearing the slot.
     *
     * @return true if an item was popped, false if empty.
     */
    // existing one-arg popHead for old paths
    boolean popHead(Consumer<T> consumer);

    // ---- Head accessors (valid only when !isEmpty())
    T headT();

    String headCorrId();

    long headStartedAtNanos();

    // ---- State
    boolean isEmpty();

    boolean isFull();
}
