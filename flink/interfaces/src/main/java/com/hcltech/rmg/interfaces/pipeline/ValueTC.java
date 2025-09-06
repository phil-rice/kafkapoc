package com.hcltech.rmg.interfaces.pipeline;

/** A typeclass allowing us to manipulate Value<T> generically. */
public interface ValueTC<T> {
    String domainId(T value);
    String msgId(T value);
}
