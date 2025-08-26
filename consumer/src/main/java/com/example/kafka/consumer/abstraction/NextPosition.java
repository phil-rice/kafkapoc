package com.example.kafka.consumer.abstraction;

/** Compute the commit/ack position for a message. */
@FunctionalInterface
public interface NextPosition<M, P> {
    P of(M message);
}
