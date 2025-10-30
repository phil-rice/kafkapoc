package com.hcltech.rmg.common.copy;

import java.util.*;

@SuppressWarnings("unchecked")
public final class MapObjectDeepCopy implements DeepCopy<Map<String, Object>> {

    @Override
    public Map<String, Object> copy(Map<String, Object> value) {
        if (value == null) return null;
        return copyMap(value);
    }

    private Map<String, Object> copyMap(Map<String, Object> src) {
        Map<String, Object> dst = new HashMap<>(Math.max(16, (int)(src.size() / 0.75f) + 1));
        for (Map.Entry<String, Object> e : src.entrySet()) {
            dst.put(e.getKey(), copyAny(e.getValue()));
        }
        return dst;
    }

    private Object copyAny(Object v) {
        if (v == null) return null;

        if (v instanceof Map<?, ?> m) {
            // assume nested maps are also Map<String,Object>
            return copyMap((Map<String, Object>) m);
        }
        if (v instanceof List<?> list) {
            return copyList(list);
        }
        if (v instanceof Set<?> set) {
            // optional: keep type; here we preserve insertion order via LinkedHashSet
            return copySet(set);
        }
        if (v instanceof Object[] arr) {
            return copyArray(arr);
        }

        // Strings, boxed numbers/booleans, enums are immutable; return as-is
        // For custom mutable objects, add type-specific copy logic here.
        return v;
    }

    private List<Object> copyList(List<?> src) {
        List<Object> dst = new ArrayList<>(src.size());
        for (Object o : src) dst.add(copyAny(o));
        return dst;
    }

    private Set<Object> copySet(Set<?> src) {
        Set<Object> dst = new LinkedHashSet<>(Math.max(16, (int)(src.size() / 0.75f) + 1));
        for (Object o : src) dst.add(copyAny(o));
        return dst;
    }

    private Object[] copyArray(Object[] src) {
        Object[] dst = new Object[src.length];
        for (int i = 0; i < src.length; i++) dst[i] = copyAny(src[i]);
        return dst;
    }
}
