package com.hcltech.rmg.cepstate;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class CepEventForMapStringObjectTest {

    private static ObjectMapper mapper() {
        ObjectMapper m = new ObjectMapper();
        m.findAndRegisterModules();
        return m;
    }

    private final CepStateTypeClass<Map<String,Object>> typeClass = new MapStringObjectCepStateTypeClass();

    // ----------------- Polymorphic (de)serialization: single events -----------------

    @Test
    void serializeSetEvent_containsTypeAndFields() throws Exception {
        ObjectMapper m = mapper();

        CepEvent e = CepEvent.set(List.of("person", "name", "first"), "Alice");
        String json = m.writeValueAsString(e);

        // quick structural checks
        assertTrue(json.contains("\"type\":\"set\""));
        assertTrue(json.contains("\"path\":[\"person\",\"name\",\"first\"]"));
        assertTrue(json.contains("\"value\":\"Alice\""));

        CepEvent back = m.readValue(json, CepEvent.class);
        assertTrue(back instanceof CepSetEvent);
        CepSetEvent s = (CepSetEvent) back;
        assertEquals(List.of("person", "name", "first"), s.path());
        assertEquals("Alice", s.value());
    }

    @Test
    void serializeAppendEvent_containsTypeAndFields() throws Exception {
        ObjectMapper m = mapper();

        CepEvent e = CepEvent.append(List.of("tags"), "alpha");
        String json = m.writeValueAsString(e);

        assertTrue(json.contains("\"type\":\"append\""));
        assertTrue(json.contains("\"path\":[\"tags\"]"));
        assertTrue(json.contains("\"value\":\"alpha\""));

        CepEvent back = m.readValue(json, CepEvent.class);
        assertTrue(back instanceof CepAppendEvent);
        CepAppendEvent a = (CepAppendEvent) back;
        assertEquals(List.of("tags"), a.path());
        assertEquals("alpha", a.value());
    }

    // ----------------- processState behavior: set -----------------

    @Test
    void process_set_createsIntermediatesAndSetsLeaf() {
        Map<String, Object> state = new HashMap<>();
        CepEvent e = CepEvent.set(List.of("person", "name", "first"), "Alice");

        Map<String, Object> after = typeClass.processState(state, e);

        assertEquals("Alice", valueAt(after, List.of("person", "name", "first")));
        assertTrue(((Map<?, ?>) ((Map<?, ?>) after.get("person")).get("name")) instanceof Map);
    }

    @Test
    void process_set_overwritesIntermediateNonMap() {
        Map<String, Object> state = new HashMap<>();
        state.put("person", "not-a-map");

        CepEvent e = CepEvent.set(List.of("person", "name", "first"), "Alice");
        Map<String, Object> after = typeClass.processState(state, e);

        assertTrue(after.get("person") instanceof Map);
        assertEquals("Alice", valueAt(after, List.of("person", "name", "first")));
    }

    @Test
    void process_set_overwritesLeafWithDifferentType() {
        Map<String, Object> state = new HashMap<>();
        state.put("k", new HashMap<>(Map.of("leaf", 123)));

        CepEvent e = CepEvent.set(List.of("k", "leaf"), List.of("x", "y"));
        Map<String, Object> after = typeClass.processState(state, e);

        assertEquals(List.of("x", "y"), valueAt(after, List.of("k", "leaf")));
    }

    @Test
    void process_set_emptyPath_isNoOp() {
        Map<String, Object> state = new HashMap<>();
        state.put("unchanged", true);

        CepEvent e = CepEvent.set(List.of(), "ignored");
        Map<String, Object> after = typeClass.processState(state, e);

        assertEquals(Map.of("unchanged", true), after);
    }

    // ----------------- processState behavior: append -----------------

    @Test
    void process_append_createsListWhenMissing() {
        Map<String, Object> state = new HashMap<>();

        CepEvent e = CepEvent.append(List.of("items"), "a");
        Map<String, Object> after = typeClass.processState(state, e);

        assertEquals(List.of("a"), valueAt(after, List.of("items")));
    }

    @Test
    void process_append_overwritesNonListWithNewList() {
        Map<String, Object> state = new HashMap<>();
        state.put("items", "not-a-list");

        CepEvent e = CepEvent.append(List.of("items"), "a");
        Map<String, Object> after = typeClass.processState(state, e);

        assertEquals(List.of("a"), valueAt(after, List.of("items")));
    }

    @Test
    void process_append_appendsWhenListExists() {
        Map<String, Object> state = new HashMap<>();
        state.put("items", new ArrayList<>(List.of("a")));

        CepEvent e = CepEvent.append(List.of("items"), "b");
        Map<String, Object> after = typeClass.processState(state, e);

        assertEquals(List.of("a", "b"), valueAt(after, List.of("items")));
    }

    @Test
    void process_append_createsIntermediates() {
        Map<String, Object> state = new HashMap<>();

        CepEvent e = CepEvent.append(List.of("person", "tags"), "new");
        Map<String, Object> after = typeClass.processState(state, e);

        assertEquals(List.of("new"), valueAt(after, List.of("person", "tags")));
    }

    @Test
    void process_append_emptyPath_isNoOp() {
        Map<String, Object> state = new HashMap<>();
        state.put("keep", 1);

        CepEvent e = CepEvent.append(List.of(), "x");
        Map<String, Object> after = typeClass.processState(state, e);

        assertEquals(1, after.get("keep"));
        assertEquals(1, after.size());
    }

    // ----------------- Optional: codec sanity (enable if your ErrorsOr API matches) -----------------
    // @Test
    // void codec_roundtrip_withCodex() {
    //     var encoded = CepEvent.codex.encode(CepEvent.set(List.of("a"), 42));
    //     assertTrue(encoded.isOk(), "encode should succeed: " + encoded.errors());
    //
    //     var decoded = CepEvent.codex.decode(encoded.value());
    //     assertTrue(decoded.isOk(), "decode should succeed: " + decoded.errors());
    //     assertTrue(decoded.value() instanceof CepSetEvent);
    // }

    // ----------------- helpers -----------------

    @SuppressWarnings("unchecked")
    private static Object valueAt(Map<String, Object> root, List<String> path) {
        Object current = root;
        for (String p : path) {
            if (!(current instanceof Map)) return null;
            Map<String, Object> m = (Map<String, Object>) current;
            current = m.get(p);
            if (current == null) return null;
        }
        return current;
    }
}
