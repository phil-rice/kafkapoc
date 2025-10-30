package com.hcltech.rmg.cepstate.worklease;

import org.jetbrains.annotations.Nullable;

import java.io.Serializable;

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
 *   <li><b>Fencing:</b> {@code token} must match the active lease for {@code domainId}
 *   <li><b>Scope:</b> One {@code WorkLease} instance is intended <b>per Kafka partition</b>.
 *       (If you share across partitions, incorporate the partition in your keying at the adapter.)</li>
 * </ul>
 *
 */
public interface WorkLease<Msg> extends Serializable {
    /** Returns a fencing token if acquired (domain idle), else null (message queued). */
    @Nullable String tryAcquire(String domainId, Msg message); // nullable

    /** Finish current item successfully. Returns next message+new token if backlog exists, else null. */
     HandBackTokenResult<Msg> succeed(String domainId, String token); // nullable

    /** Finish current item as failed/deferred. Same handoff semantics as succeed. */
     HandBackTokenResult<Msg> fail(String domainId, String token); // nullable

    static <Msg>WorkLease<Msg> memory(ITokenGenerator tokenGen) {
        return new MemoryWorkLease<>(tokenGen);
    }
}