package com.hcltech.rmg.dag;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public abstract class PathTCContractTest<P> {
    protected abstract PathTC<P> ptc();
    protected abstract P p(String path);

    @Test
    void isPrefix_segmentAware() {
        var c = ptc();
        assertTrue(c.isPrefix(p("a.b"), p("a.b")));
        assertTrue(c.isPrefix(p("a.b"), p("a.b.c")));
        assertFalse(c.isPrefix(p("a.b"), p("a.bc")));
        assertFalse(c.isPrefix(p("a.b.c"), p("a.b")));
    }

    @Test void compare_deeperGreater_thenLexicographic() {
        var c = ptc();
        assertTrue(c.compare(p("a.b.c"), p("a.b")) > 0);
        assertTrue(c.compare(p("a.b"), p("a")) > 0);
        assertTrue(c.compare(p("a.b"), p("a.c")) < 0);
        assertEquals(0, c.compare(p("a.b.c"), p("a.b.c")));
    }

    @Test void nearestOwner_picksDeepest() {
        var c = ptc();
        var roots = List.of(p("a"), p("a.b"), p("x.y"));
        assertEquals(p("a.b"), c.nearestOwner(p("a.b.c"), roots).orElseThrow());
    }
}
 final class StringPathTCContractTest extends PathTCContractTest<String> {
    @Override protected PathTC<String> ptc() { return new StringPathTC(); }
    @Override protected String p(String path) { return path; }
}

 final class ListPathTCContractTest extends PathTCContractTest<List<String>> {
    @Override protected PathTC<List<String>> ptc() { return new ListPathTC(); }
    @Override protected List<String> p(String path) {
        return path.isEmpty() ? List.of() : Arrays.asList(path.split("\\.", -1));
    }
}
