package com.hcltech.rmg.common.async;

/**
 * Lock-free multi-producer single-consumer ring.
 * Many producers offer events; a single consumer drains them on the operator thread.
 */
public interface IMpscRing<In, Out> {

    boolean offerSuccess(In in, long corrId, Out out);

    boolean offerFailure(In in, long corrId, Throwable error);

    /**
     * Drain as many events as available, invoking the handler for each.
     * @return number of drained events
     */
    int drain(Handler<In, Out> handler);

    /**
     * Drain at most one event.
     * @return a single event, or null if empty
     */
    Event<In, Out> pollOne();

    interface Handler<In, Out> {
        void onSuccess(In in, long corrId, Out out);
        void onFailure(In in, long corrId, Throwable error);
    }

    /** Single drained event (for simple tests). */
    final class Event<In, Out> {
        public final boolean success;
        public final In in;
        public final long corrId;
        public final Out out;               // when success
        public final Throwable error;       // when failure
        public Event(boolean success, In in, long corrId, Out out, Throwable error) {
            this.success = success; this.in = in; this.corrId = corrId; this.out = out; this.error = error;
        }
    }
}
