package com.hcltech.rmg.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface Paths {

    static Object getObject(Map<String, Object> root, List<String> path) {
        if (root == null || path == null) return null;
        if (path.isEmpty()) return root;

        Object current = root;
        for (String p : path) {
            if (!(current instanceof Map)) return null;
            @SuppressWarnings("unchecked")
            Map<String, Object> m = (Map<String, Object>) current;
            if (!m.containsKey(p)) return null;
            current = m.get(p);
        }
        return current;
    }

    static String findStringOrNull(Map<String, Object> root, List<String> path) {
        Object v = getObject(root, path);
        return (v instanceof String) ? (String) v : null;
    }

    static Map<String, Object> setValue(Map<String, Object> root, List<String> path, Object value) {
        if (root == null) throw new IllegalArgumentException("root cannot be null");
        if (path == null) throw new IllegalArgumentException("path cannot be null");

        if (path.isEmpty()) return root;

        Map<String, Object> current = root;

        for (int i = 0; i < path.size() - 1; i++) {
            String segment = path.get(i);
            Object next = current.get(segment);
            if (!(next instanceof Map)) {
                next = new HashMap<String, Object>();
                current.put(segment, next);
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> asMap = (Map<String, Object>) next;
            current = asMap;
        }

        current.put(path.get(path.size() - 1), value);
        return root;
    }

    /**
     * Appends 'value' to a list at the given path.
     * - Empty path: NO-OP; returns the same root.
     * - Creates intermediate maps as needed (permissive).
     * - If the leaf doesn't exist or is not a List, overwrites it with a new List containing only 'value'.
     * - If the leaf is a List, appends 'value'.
     */
    static Map<String, Object> appendValue(Map<String, Object> root, List<String> path, Object value) {
        if (root == null) throw new IllegalArgumentException("root cannot be null");
        if (path == null) throw new IllegalArgumentException("path cannot be null");

        if (path.isEmpty()) return root;

        Map<String, Object> current = root;

        // Walk/create intermediates
        for (int i = 0; i < path.size() - 1; i++) {
            String segment = path.get(i);
            Object next = current.get(segment);
            if (!(next instanceof Map)) {
                next = new HashMap<String, Object>();
                current.put(segment, next);
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> asMap = (Map<String, Object>) next;
            current = asMap;
        }

        // Handle leaf
        String leafKey = path.get(path.size() - 1);
        Object existing = current.get(leafKey);
        if (existing instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) existing;
            list.add(value);
        } else {
            List<Object> list = new ArrayList<>();
            list.add(value);
            current.put(leafKey, list);
        }

        return root;
    }
}
