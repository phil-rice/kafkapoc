package com.example.cepstate;

/**
 * Coordinates exclusive processing of a domain item while preserving partition order.
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li><b>Acquire:</b> If there is no active lease <i>and</i> the backlog is empty,
 *       acquire and return a non-null fencing token. Otherwise, enqueue the offset (FIFO) and return {@code null}.</li>
 *   <li><b>Fail:</b> Caller keeps the lease; the store records failure. Any retry/backoff policy
 *       (timers, pause/resume) is handled elsewhere.</li>
 *   <li><b>Succeed:</b> Release the lease; if backlog exists, atomically hand ownership to the head.
 *       The next unit can be picked up on the next poll (or immediately if you resume).</li>
 *   <li><b>Fencing:</b> {@code token} must match the active lease for {@code domainId}, otherwise
 *       implementations should throw {@link IllegalStateException}.</li>
 *   <li><b>Scope:</b> One {@code WorkLease} instance is intended <b>per Kafka partition</b>.
 *       (If you share across partitions, incorporate the partition in your keying at the adapter.)</li>
 * </ul>
 *

 */
public interface WorkLease {

    /**
     * Attempt to acquire the lease for {@code (domainId, offset)}.
     * <p>Atomic rules:</p>
     * <ul>
     *   <li>If no active lease and backlog empty → acquire now and return a non-null token.</li>
     *   <li>Else → enqueue {@code offset} (FIFO) and return {@code null}.</li>
     * </ul>
     *
     * Behind the scenes this will check to see if the message has been processed (idempotent check)
     * or if there is already a lease for the domain id. In the later case it will queue the offset
     *
     * if this returns null the calling code should stop
     *
     * @param domainId application key that must be processed serially on this partition
     * @param offset   partition-local Kafka offset of the record
     * @return opaque fencing token if acquired; {@code null} if queued
     */
    String tryAcquire(String domainId, long offset);

    /**
     * Work finished successfully.
     * <p>Atomically releases the lease; if backlog is non-empty, hands the lease
     * to the next offset so it can be picked up on the next poll.</p>
     *
     * Behind the scenes, if some other offset for the same domain id tried to get a lease and failed, it will be triggered
     *
     * The calling code shouldn't do anything after this.
     *
     * @param domainId same id passed to {@link #tryAcquire}
     * @param token    fencing token previously returned by {@link #tryAcquire}
     *
     * Note if the token fails to match we do nothing. It is probably because another retry has taken over the lease.
     */
    void succeed(String domainId, String token);

    /**
     * Work failed (or is being deferred). Caller retains the lease; new offsets keep queueing.
     * <p>Any retry schedule (ladder/backoff), timers, and pause/resume decisions are handled outside this interface.</p>
     *
     * Behind the scenes this method will either trigger a retry in the future or give up
     *
     * The calling code should stop after calling this
     *
     * @param domainId same id passed to {@link #tryAcquire}
     * @param token    fencing token previously returned by {@link #tryAcquire}
     *
     * Note if the token fails to match we do nothing. It is probably because another retry has taken over the lease.
     */
    void fail(String domainId, String token);
}
