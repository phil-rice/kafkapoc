package com.hcltech.rmg.cepstate;

import com.hcltech.rmg.common.Paths;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/** Contract for CepEventLog semantics. */
public interface AbstractCepEventLogContractTest {

    /** Supply a fresh instance per test. */
    CepEventLog newLog();

    // helpers
    private static CepEvent set(List<String> p, Object v) { return CepEvent.set(p, v); }
    private static CepEvent app(List<String> p, Object v) { return CepEvent.append(p, v); }

    @BeforeEach
    default void beforeEachContract() { /* no-op */ }

    @Test
    default void append_emptyOrNull_isNoOp() throws Exception {
        var log = newLog();
        log.append(Collections.emptyList());
        log.append(null);
        assertTrue(log.getAll().isEmpty());
    }

    @Test
    default void append_singleBatch_thenGetAll_preservesOrder() throws Exception {
        var log = newLog();
        var batch = List.of(set(List.of("a"), 1), app(List.of("list"), "x"), set(List.of("b","c"), true));
        log.append(batch);
        assertEquals(batch, log.getAll());
    }

    @Test
    default void append_multipleBatches_preservesGlobalOrder() throws Exception {
        var log = newLog();
        var b1 = List.of(set(List.of("k"), "v1"), app(List.of("tags"), "t1"));
        var b2 = List.of(set(List.of("k"), "v2"));
        var b3 = List.of(app(List.of("tags"), "t2"), app(List.of("tags"), "t3"));
        log.append(b1); log.append(b2); log.append(b3);
        assertEquals(List.of(b1.get(0), b1.get(1), b2.get(0), b3.get(0), b3.get(1)), log.getAll());
    }

    @Test
    default void append_defensiveCopyOfInput() throws Exception {
        var log = newLog();
        var batch = new ArrayList<CepEvent>();
        batch.add(set(List.of("x"), 1));
        log.append(batch);
        batch.clear();
        var all = log.getAll();
        assertEquals(1, all.size());
        assertEquals(set(List.of("x"), 1), all.get(0));
    }

    @Test
    default void foldAll_materializesStateCorrectly() throws Exception {
        var log = newLog();
        log.append(List.of(set(List.of("person","name"), "Alice"), app(List.of("tags"), "a")));
        log.append(List.of(set(List.of("person","age"), 30), app(List.of("tags"), "b")));
        log.append(List.of(set(List.of("person","name"), "Alicia")));
        var state = new HashMap<String,Object>();
        var out = log.foldAll(state);
        assertSame(state, out);
        assertEquals("Alicia", Paths.getObject(state, List.of("person","name")));
        assertEquals(30, Paths.getObject(state, List.of("person","age")));
        assertEquals(List.of("a","b"), Paths.getObject(state, List.of("tags")));
    }
}
