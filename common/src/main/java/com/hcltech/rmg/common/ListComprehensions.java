package com.hcltech.rmg.common;

import java.util.Collection;

public interface ListComprehensions {
    public static <T, R> java.util.List<R> map(java.util.List<T> list, java.util.function.Function<T, R> mapper) {
        java.util.List<R> result = new java.util.ArrayList<>();
        for (T item : list) {
            result.add(mapper.apply(item));
        }
        return result;
    }

    public static <T, R> java.util.List<R> flatMao(java.util.List<T> list, java.util.function.Function<T, ? extends Collection<R>> mapper) {
        java.util.List<R> result = new java.util.ArrayList<>();
        for (T item : list) {
            result.addAll(mapper.apply(item));
        }
        return result;
    }
}
