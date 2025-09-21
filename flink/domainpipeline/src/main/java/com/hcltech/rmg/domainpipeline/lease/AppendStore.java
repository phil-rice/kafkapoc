package com.hcltech.rmg.domainpipeline.lease;

import java.util.List;
import java.util.Optional;

/**
 * A simple append-only, per-key queue abstraction.
 *
 * <p>Intended to back a lease coordinator:
 *  - {@link #append} adds new items in arrival order
 *  - {@link #peek} looks at the head (oldest) item without removing it
 *  - {@link #pop} removes and returns the head item
 *  - {@link #getAll} is a read-only snapshot of the entire backlog
 *  - {@link #deleteAll} wipes the entire queue for a key</p>
 *
 * <p>Implementations can be in-memory (for tests) or durable (e.g. RocksDB, CEP store).
 * API deliberately keeps mutation minimal: append, pop head, or delete all.</p>
 *
 * @param <T> type of items to store
 */
public interface AppendStore<T> {

    /**
     * Append a value to the end of the queue for this key.
     *
     * <p>Must preserve arrival order: the first appended value is the first to
     * be returned by {@link #peek} or {@link #pop}.</p>
     *
     * @param key logical domain identifier (e.g. "partitionId:domainId")
     * @param value value to enqueue
     */
    void append(String key, T value);

    /**
     * Look at the current head element without removing it.
     *
     * @param key logical domain identifier
     * @return the oldest value if present, otherwise {@link Optional#empty()}
     */
    Optional<T> peek(String key);

    /**
     * Remove and return the current head element.
     *
     * <p>If the queue is empty, returns {@link Optional#empty()}.
     * Semantics are FIFO: the element returned here is the same one you would
     * have seen via {@link #peek} immediately before this call.</p>
     *
     * @param key logical domain identifier
     * @return the removed element, or empty if none
     */
    Optional<T> pop(String key);

    /**
     * Return a snapshot of all queued values for this key.
     *
     * <p>Use for metrics, debugging, or special-case batch handling.
     * Implementations may copy values into a new list — do not assume live view.</p>
     *
     * @param key logical domain identifier
     * @return list of all queued values in FIFO order; never null
     */
    List<T> getAll(String key);

    /**
     * Delete all entries for this key (clear the queue).
     *
     * <p>Use with care — intended for error handling, retries, or domain reset.
     * After this call, {@link #peek}, {@link #pop}, and {@link #getAll}
     * will return empty results until new values are appended.</p>
     *
     * @param key logical domain identifier
     */
    void deleteAll(String key);
}
