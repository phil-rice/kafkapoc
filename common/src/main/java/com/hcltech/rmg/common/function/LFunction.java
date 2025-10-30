package com.hcltech.rmg.common.function;

import java.io.Serializable;

@FunctionalInterface
public interface LFunction<E> extends Serializable {
    long apply(E e); // return < 0 to indicate "no timestamp"
}
