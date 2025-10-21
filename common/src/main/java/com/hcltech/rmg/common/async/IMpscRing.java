package com.hcltech.rmg.common.async;

/**
 * Lock-free multi-producer single-consumer ring.
 * Many producers offer events; a single consumer drains them on the operator thread.
 */
public interface IMpscRing<FR, In, Out> {

    boolean offerSuccess(FR fr, In in, String corrId, Out out);

    boolean offerFailure(FR fr, In in, String corrId, Throwable error);

    /**
     * Drain as many events as available, invoking the handler for each.
     * @return number of drained events
     */
    int drain(Handler<FR, In, Out> handler);

    interface Handler<FR, In, Out> {
        void onSuccess(FR fr, In in, String corrId, Out out);
        void onFailure(FR fr, In in, String corrId, Throwable error);
    }
}
