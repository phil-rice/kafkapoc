package com.hcltech.rmg.common.async;

import com.hcltech.rmg.common.function.TriConsumer;

import java.util.function.BiConsumer;

/**
 * Minimal single-threaded lane API for the operator thread.
 */
public interface ILane<FR,T> {

    // ---- Mutations
    /** Enqueue at tail. Caller must ensure !isFull(). */
    void enqueue(T t, FR fr,long correlationId, long startedAtNanos);

    /** Pop head if present; apply callback with FR, T and an external context. */
    <C> boolean popHead(C ctx, TriConsumer<FR,T,C> consumer);


    /**
     * Pop head if present, clearing the slot.
     * @return true if an item was popped, false if empty.
     */
    // existing one-arg popHead for old paths
    boolean popHead(BiConsumer<FR,T> consumer);

    // ---- Head accessors (valid only when !isEmpty())
    T headT();
    FR headFR();
    long headCorrId();
    long headStartedAtNanos();

    // ---- State
    boolean isEmpty();
    boolean isFull();
}
