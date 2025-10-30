package com.hcltech.rmg.common;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PathsTest {

    // -------- getObject / findStringOrNull --------

    @Test
    void getObject_returnsRootOnEmptyPath() {
        Map<String, Object> root = new HashMap<>();
        root.put("a", 1);
        Object result = Paths.getObject(root, List.of());
        assertSame(root, result);
    }

    @Test
    void getObject_returnsNullWhenPathMissing() {
        Map<String, Object> root = Map.of("person", Map.of("name", Map.of("first", "Alice")));
        assertNull(Paths.getObject(root, List.of("person", "age")));
        assertNull(Paths.getObject(root, List.of("person", "name", "last")));
    }

    @Test
    @SuppressWarnings("unchecked")
    void getObject_returnsNullWhenIntermediateNotMap() {
        Map<String, Object> root = new HashMap<>();
        root.put("person", "Not a map");
        assertNull(Paths.getObject(root, List.of("person", "name")));

        // sanity: the root remains unchanged
        assertEquals("Not a map", root.get("person"));
    }

    @Test
    void getObject_supportsAnyLeafType() {
        Map<String, Object> root = new HashMap<>();
        root.put("num", 42);
        root.put("list", List.of(1, 2, 3));
        root.put("map", Map.of("k", "v"));

        assertEquals(42, Paths.getObject(root, List.of("num")));
        assertEquals(List.of(1, 2, 3), Paths.getObject(root, List.of("list")));
        assertEquals("v", ((Map<?, ?>) Paths.getObject(root, List.of("map"))).get("k"));
    }

    @Test
    void findStringOrNull_returnsStringOrNull() {
        Map<String, Object> root = new HashMap<>();
        root.put("str", "hello");
        root.put("num", 123);

        assertEquals("hello", Paths.findStringOrNull(root, List.of("str")));
        assertNull(Paths.findStringOrNull(root, List.of("num")));     // not a string
        assertNull(Paths.findStringOrNull(root, List.of("missing"))); // missing
    }

    @Test
    void getObject_nullRootOrNullPath_returnsNull() {
        Map<String, Object> root = new HashMap<>();
        root.put("x", 1);
        assertNull(Paths.getObject(null, List.of("x")));
        assertNull(Paths.getObject(root, null));
    }

    // -------- setValue (root remains Map<String,Object>) --------

    @Test
    void setValue_emptyPath_isNoOp_returnsSameRoot() {
        Map<String, Object> root = new HashMap<>();
        root.put("keep", "me");
        Map<String, Object> result = Paths.setValue(root, List.of(), "IGNORED");

        assertSame(root, result);
        assertEquals("me", root.get("keep"));
        assertEquals(1, root.size());
    }

    @Test
    void setValue_singleSegment_setsAtRoot() {
        Map<String, Object> root = new HashMap<>();
        Paths.setValue(root, List.of("title"), "Engineer");

        assertEquals("Engineer", Paths.findStringOrNull(root, List.of("title")));
    }

    @Test
    void setValue_createsIntermediatesAndSetsLeaf() {
        Map<String, Object> root = new HashMap<>();

        Map<String, Object> result = Paths.setValue(root, List.of("person", "name", "first"), "Alice");
        assertSame(root, result);

        assertEquals("Alice", Paths.findStringOrNull(root, List.of("person", "name", "first")));

        // Intermediates are maps
        assertTrue(root.get("person") instanceof Map);
        Map<?, ?> person = (Map<?, ?>) root.get("person");
        assertTrue(person.get("name") instanceof Map);
    }

    @Test
    void setValue_overwritesIntermediateNonMap_convertsToMap() {
        Map<String, Object> root = new HashMap<>();
        root.put("person", "Not a map");

        Paths.setValue(root, List.of("person", "name", "first"), "Alice");

        assertTrue(root.get("person") instanceof Map);
        Map<?, ?> person = (Map<?, ?>) root.get("person");
        assertTrue(person.get("name") instanceof Map);
        Map<?, ?> name = (Map<?, ?>) person.get("name");
        assertEquals("Alice", name.get("first"));
    }

    @Test
    void setValue_overwritesExistingLeafString() {
        Map<String, Object> root = new HashMap<>();
        root.put("person", new HashMap<>(Map.of(
                "name", new HashMap<>(Map.of("first", "Bob"))
        )));

        Paths.setValue(root, List.of("person", "name", "first"), "Alice");
        assertEquals("Alice", Paths.findStringOrNull(root, List.of("person", "name", "first")));
    }

    @Test
    void setValue_overwritesLeafWithDifferentType() {
        Map<String, Object> root = new HashMap<>();
        root.put("person", new HashMap<>(Map.of(
                "name", new HashMap<>(Map.of("first", "Bob"))
        )));

        Paths.setValue(root, List.of("person", "name", "first"), List.of(1, 2));
        assertEquals(List.of(1, 2), Paths.getObject(root, List.of("person", "name", "first")));
    }

    @Test
    void setValue_throwsOnNullRootOrNullPath() {
        assertThrows(IllegalArgumentException.class,
                () -> Paths.setValue(null, List.of("x"), 1));

        Map<String, Object> root = new HashMap<>();
        assertThrows(IllegalArgumentException.class,
                () -> Paths.setValue(root, null, 1));
    }
    // -------- appendValue --------

    @Test
    void appendValue_createsListWhenMissing() {
        Map<String, Object> root = new HashMap<>();

        Paths.appendValue(root, List.of("items"), "a");

        Object v = Paths.getObject(root, List.of("items"));
        assertTrue(v instanceof List);
        assertEquals(List.of("a"), v);
    }

    @Test
    void appendValue_overwritesNonListWithNewList() {
        Map<String, Object> root = new HashMap<>();
        root.put("items", "not-a-list");

        Paths.appendValue(root, List.of("items"), "a");

        Object v = Paths.getObject(root, List.of("items"));
        assertTrue(v instanceof List);
        assertEquals(List.of("a"), v); // previous non-list value is NOT preserved
    }

    @Test
    void appendValue_appendsWhenListExists() {
        Map<String, Object> root = new HashMap<>();
        root.put("items", new java.util.ArrayList<>(List.of("a")));

        Paths.appendValue(root, List.of("items"), "b");

        assertEquals(List.of("a", "b"), Paths.getObject(root, List.of("items")));
    }

    @Test
    void appendValue_createsIntermediatesAndAppends() {
        Map<String, Object> root = new HashMap<>();

        Paths.appendValue(root, List.of("person", "tags"), "new");
        Object tags = Paths.getObject(root, List.of("person", "tags"));

        assertTrue(((Map<?, ?>) root.get("person")).get("tags") instanceof List);
        assertEquals(List.of("new"), tags);
    }

    @Test
    void appendValue_replacesIntermediateNonMap() {
        Map<String, Object> root = new HashMap<>();
        root.put("person", "not-a-map");

        Paths.appendValue(root, List.of("person", "tags"), "x");

        Object person = root.get("person");
        assertTrue(person instanceof Map);
        assertEquals(List.of("x"), Paths.getObject(root, List.of("person", "tags")));
    }

    @Test
    void appendValue_allowsNullElement() {
        Map<String, Object> root = new HashMap<>();

        Paths.appendValue(root, List.of("items"), null);

        assertEquals(1, ((List<?>) Paths.getObject(root, List.of("items"))).size());
        assertNull(((List<?>) Paths.getObject(root, List.of("items"))).get(0));
    }

    @Test
    void appendValue_emptyPath_isNoOp_returnsSameRoot() {
        Map<String, Object> root = new HashMap<>();
        root.put("keep", 1);

        Map<String, Object> result = Paths.appendValue(root, List.of(), "x");

        assertSame(root, result);
        assertEquals(1, root.get("keep"));
        assertEquals(1, root.size());
    }

    @Test
    void appendValue_throwsOnNullRootOrNullPath() {
        assertThrows(IllegalArgumentException.class,
                () -> Paths.appendValue(null, List.of("x"), 1));

        Map<String, Object> root = new HashMap<>();
        assertThrows(IllegalArgumentException.class,
                () -> Paths.appendValue(root, null, 1));
    }

}
