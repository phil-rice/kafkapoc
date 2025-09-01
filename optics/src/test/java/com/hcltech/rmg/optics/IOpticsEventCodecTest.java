// src/test/java/com/example/optics/IOpticsEventCodecTest.java
package com.hcltech.rmg.optics;

import com.hcltech.rmg.common.Codec;
import org.apache.commons.jxpath.JXPathContext;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class IOpticsEventCodecTest {

    private Codec<IOpticsEvent<JXPathContext>, String> codec() {
        return IOpticsEvent.codec();
    }

    // ---------- apply() behavior ----------

    @Test
    void setEvent_apply_updates_context_path() {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("user", new LinkedHashMap<String, Object>());

        JXPathContext ctx = JXPathContext.newContext(root);

        IOpticsEvent<JXPathContext> ev = new SetEvent<>("user/name", "Alice");
        ev.apply(ctx);

        assertEquals("Alice", ctx.getValue("user/name"));
    }

    @Test
    void appendEvent_apply_appends_to_list() {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("scores", new ArrayList<>(List.of(10, 20)));

        JXPathContext ctx = JXPathContext.newContext(root);

        IOpticsEvent<JXPathContext> ev = new AppendEvent<>("scores", 30);
        ev.apply(ctx);

        Object got = ctx.getValue("scores");
        assertInstanceOf(List.class, got);
        assertEquals(List.of(10, 20, 30), got);
    }

    // ---------- codec round-trips (String) ----------

    @Test
    void codec_roundTrip_setEvent_string() throws Exception {
        var c = codec();

        IOpticsEvent<JXPathContext> in = new SetEvent<>("user/name", "Alice");
        String json = c.encode(in);

        // sanity: wrapper JSON should contain type + payload and inner fields
        assertTrue(json.contains("\"type\":\"set\""));
        assertTrue(json.contains("\"payload\""));
        assertTrue(json.contains("\"path\":\"user/name\""));
        assertTrue(json.contains("\"value\":\"Alice\""));

        IOpticsEvent<JXPathContext> out = c.decode(json);
        assertInstanceOf(SetEvent.class, out);

        SetEvent<?> se = (SetEvent<?>) out;
        assertEquals("user/name", se.path());
        assertEquals("Alice", se.value());
    }

    @Test
    void codec_roundTrip_appendEvent_string() throws Exception {
        Codec<IOpticsEvent<JXPathContext>, String> c = codec();

        IOpticsEvent<JXPathContext> in = new AppendEvent<>("scores", 99);
        String json = c.encode(in);

        assertTrue(json.contains("\"type\":\"append\""));
        assertTrue(json.contains("\"payload\""));
        assertTrue(json.contains("\"path\":\"scores\""));
        assertTrue(json.contains("\"value\":99"));

        IOpticsEvent<JXPathContext> out = c.decode(json);
        assertInstanceOf(AppendEvent.class, out);

        AppendEvent<?> ae = (AppendEvent<?>) out;
        assertEquals("scores", ae.path());
        assertEquals(99, ae.value());
    }

    // ---------- end-to-end: decode, then apply ----------

    @Test
    void decode_then_apply_changes_context() throws Exception {
        Codec<IOpticsEvent<JXPathContext>, String> c = codec();

        // encode a set event
        String json = c.encode(new SetEvent<>("user/name", "Bob"));

        // fresh context
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("user", new LinkedHashMap<String, Object>());
        JXPathContext ctx = JXPathContext.newContext(root);

        IOpticsEvent<JXPathContext> decoded = c.decode(json);
        decoded.apply(ctx);

        assertEquals("Bob", ctx.getValue("user/name"));
    }

    // ---------- failure path: unknown event at encode time ----------

    @Test
    void encoding_unknown_event_throws() {
        Codec<IOpticsEvent<JXPathContext>, String> c = codec();

        // Local unknown event type to trigger discriminator failure
        record UnknownEvent() implements IOpticsEvent<JXPathContext> {
            @Override public JXPathContext apply(JXPathContext initial) { return initial; }
        }

        IOpticsEvent<JXPathContext> ev = new UnknownEvent();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> c.encode(ev));
        assertTrue(ex.getMessage().toLowerCase().contains("unknown event type"));
    }
}
