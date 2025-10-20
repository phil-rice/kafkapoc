package com.hcltech.rmg.common.async;

/**
 * Typeclass/port that knows how to complete a framework "future record".
 * Implementations must be single-use per record (complete only once).
 */
public interface FutureRecordTypeClass<FR, In, Out> {

    /** Complete successfully with a domain value. */
    void completed(FR fr, Out out);

    /** Complete with a timeout mapped to a domain value. */
    void timedOut(FR fr, In in, long elapsedNanos);

    /** Complete with an error mapped to a domain value. */
    void failed(FR fr, In in, Throwable error);
}
