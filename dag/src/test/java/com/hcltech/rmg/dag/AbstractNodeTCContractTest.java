package com.hcltech.rmg.dag;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public abstract class AbstractNodeTCContractTest<N, P> {

    protected abstract NodeTC<N, P> ntc();
    protected abstract P p(String path);
    protected abstract N makeNode(String label, String[] producesDot, String[] requiresDot);

    /** Override in concrete tests when a node type supports ONLY a single produced root. */
    protected boolean allowsMultipleProducedRoots() { return true; }

    protected Set<P> setP(String... paths) {
        Set<P> s = new LinkedHashSet<>();
        for (String v : paths) s.add(p(v));
        return s;
    }

    @Test
    public void owns_returnsExactlyProduces() {
        if (allowsMultipleProducedRoots()) {
            var n = makeNode("N", new String[]{"a.b", "x"}, new String[]{});
            assertEquals(setP("a.b", "x"), ntc().owns(n));
        } else {
            var n = makeNode("N", new String[]{"a.b"}, new String[]{});
            assertEquals(setP("a.b"), ntc().owns(n));
        }
    }

    @Test
    public void requires_returnsExactlyRequires() {
        var n = makeNode("N", new String[]{}, new String[]{"a.b.c", "x.y"});
        assertEquals(setP("a.b.c", "x.y"), ntc().requires(n));
    }

    @Test
    public void label_isNonEmpty_andStableForSameNode() {
        var n = makeNode("LabelHere", new String[]{"a.b"}, new String[]{"a.b.c"});
        String lbl1 = ntc().label(n);
        String lbl2 = ntc().label(n);
        assertNotNull(lbl1);
        assertFalse(lbl1.isBlank());
        assertEquals(lbl1, lbl2);
    }
}
