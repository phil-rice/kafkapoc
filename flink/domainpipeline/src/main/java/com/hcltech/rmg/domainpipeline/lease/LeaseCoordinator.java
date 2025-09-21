package com.hcltech.rmg.domainpipeline.lease;

/** M is message type. In flink it would be a ValueEnvelope
 * Holder is some means by which we identify the message. Might be a message id
 * */
public interface LeaseCoordinator<M> {
    /**
     * Try to acquire the lease for this domain.
     * Returns true if this holder becomes/continues to be the owner.
     * Safe to call repeatedly (idempotent for the same messageId).
     */
    boolean acquire(String domainId, String messageId, long nowMs) ;

    /**
     * Release the lease for this domain *from this holder* and return
     * the next message that should be processed for this domain (if any).
     * If the caller is not the current holder, returns Optional.empty().
     */
    java.util.Optional<M> releaseAndNext(String domainId,  String messageId, long nowMs);

}
