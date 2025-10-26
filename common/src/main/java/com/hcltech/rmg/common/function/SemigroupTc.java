package com.hcltech.rmg.common.function;

import java.util.Collection;

@FunctionalInterface
public interface SemigroupTc<Acc, V> {
    Acc add(Acc acc, V v);

    static <Acc, V> Acc add(SemigroupTc<Acc, V> tc, Collection<V> vs, Acc zero) {
        Acc acc = zero;
        for (V v : vs) acc = tc.add(acc, v);
        return acc;
    }
}