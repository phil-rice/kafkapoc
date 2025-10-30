package com.hcltech.rmg.optics;

import org.apache.commons.jxpath.JXPathContext;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class JXPathInterpreterTest {

    private <C> Interpreter<JXPathContext, C> newInterp() {
        return Interpreter.jxPathInterpreter();
    }

    @Test
    void lift_wraps_root_and_getFrom_returns_same_reference() {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("user", new LinkedHashMap<String, Object>());

        Interpreter<JXPathContext, Map<String, Object>> interp = newInterp();
        JXPathContext ctx = interp.lift(root);

        assertNotNull(ctx);
        Object dot = ctx.getValue(".");
        assertSame(root, dot, "getValue(\".\") should be the same root instance");

        Map<String, Object> back = interp.getFrom(ctx);
        assertSame(root, back, "getFrom should return the exact same root reference");
    }

    @Test
    void apply_runs_events_in_order_set_then_append() {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("user", new LinkedHashMap<String, Object>());
        root.put("scores", new ArrayList<>(List.of(10, 20)));

        Interpreter<JXPathContext, Map<String, Object>> interp = newInterp();
        JXPathContext ctx = interp.lift(root);

        List<IOpticsEvent<JXPathContext>> events = List.of(
                new SetEvent<>("user/name", "Alice"),
                new AppendEvent<>("scores", 30)
        );

        JXPathContext returned = interp.apply(events, ctx);
        assertSame(ctx, returned, "apply should return the same context instance");

        assertEquals("Alice", ctx.getValue("user/name"));
        assertEquals(List.of(10, 20, 30), ctx.getValue("scores"));
    }

    @Test
    void apply_with_only_setEvent_creates_or_updates_path() {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("user", new LinkedHashMap<String, Object>());

        Interpreter<JXPathContext, Map<String, Object>> interp = newInterp();
        JXPathContext ctx = interp.lift(root);

        interp.apply(List.of(new SetEvent<>("user/age", 42)), ctx);

        assertEquals(42, ctx.getValue("user/age"));
    }

    @Test
    void appendEvent_throws_if_path_not_a_list() {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("user", new LinkedHashMap<String, Object>());
        ((Map<String, Object>) root.get("user")).put("name", "Alice");

        Interpreter<JXPathContext, Map<String, Object>> interp = newInterp();
        JXPathContext ctx = interp.lift(root);

        IOpticsEvent<JXPathContext> badAppend = new AppendEvent<>("user/name", "SHOULD_FAIL");

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> interp.apply(List.of(badAppend), ctx)
        );
        assertTrue(ex.getMessage().contains("Path does not point to a list"));
    }

    @Test
    void factory_method_returns_jxpath_interpreter() {
        Interpreter<JXPathContext, Map<String, Object>> interp = Interpreter.jxPathInterpreter();
        assertNotNull(interp);
        assertTrue(interp instanceof JXPathInterpreter);
    }
}
