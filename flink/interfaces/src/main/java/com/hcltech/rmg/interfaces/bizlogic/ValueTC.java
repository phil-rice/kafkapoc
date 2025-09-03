package com.hcltech.rmg.interfaces.bizlogic;

/** A typeclass allowing us to manipulate Value<T> generically. */
public interface ValueTC<T> {
    String domainId(T value);
    String msgId(T value);
}
