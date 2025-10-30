package com.hcltech.rmg.common.async;

/** Typeclass to build a final Out from a failure of the async step. */
public interface FailureAdapter<In, Out> {
    Out onFailure(In input, Throwable error);
    Out onTimeout(In in, long elapsedNanos);

}
