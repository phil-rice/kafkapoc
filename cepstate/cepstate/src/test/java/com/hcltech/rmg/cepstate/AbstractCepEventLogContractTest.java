// src/test/java/com/hcltech/rmg/cepstate/AbstractCepEventLogContractTest.java
package com.hcltech.rmg.cepstate;

import com.hcltech.rmg.common.Paths;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Contract: run a test body inside a per-key CEP log context,
 * where the body only uses the simple CepEventLog API.
 */
public interface AbstractCepEventLogContractTest {

    @FunctionalInterface
    interface Body {
        void run(CepEventLog log) throws Exception;
    }

    /** Run the test body inside a log context for the given key. */
    void withLog(String key, Body body) throws Exception;

    // helpers to build events

    @Test
    default void append_emptyOrNull_isNoOp() throws Exception {
        withLog("K", log -> {
            log.append(List.of());
            log.append(null);
            assertTrue(log.getAll().isEmpty());
        });
    }

    @Test
    default void append_singleBatch_thenGetAll_preservesOrder() throws Exception {
        withLog("K", log -> {
            var batch = List.of(
                    CepEvent.set(List.of("a"), 1),
                    CepEvent.append(List.of("list"), "x"),
                    CepEvent.set(List.of("b", "c"), true)
            );
            log.append(batch);
            assertEquals(batch, log.getAll());
        });
    }

    @Test
    default void append_multipleBatches_preservesGlobalOrder() throws Exception {
        withLog("K", log -> {
            var b1 = List.of(CepEvent.set(List.of("k"), "v1"), CepEvent.append(List.of("tags"), "t1"));
            var b2 = List.of(CepEvent.set(List.of("k"), "v2"));
            var b3 = List.of(CepEvent.append(List.of("tags"), "t2"), CepEvent.append(List.of("tags"), "t3"));

            log.append(b1);
            log.append(b2);
            log.append(b3);

            assertEquals(
                    List.of(b1.get(0), b1.get(1), b2.get(0), b3.get(0), b3.get(1)),
                    log.getAll()
            );
        });
    }

    @Test
    default void append_defensiveCopyOfInput() throws Exception {
        withLog("K", log -> {
            var batch = new ArrayList<CepEvent>();
            batch.add(CepEvent.set(List.of("x"), 1));

            log.append(batch);
            batch.clear(); // must not affect stored events

            var all = log.getAll();
            assertEquals(1, all.size());
            assertEquals(CepEvent.set(List.of("x"), 1), all.get(0));
        });
    }

    @Test
    default void foldAll_materializesStateCorrectly() throws Exception {
        withLog("K", log -> {
            log.append(List.of(CepEvent.set(List.of("person", "name"), "Alice"), CepEvent.append(List.of("tags"), "a")));
            log.append(List.of(CepEvent.set(List.of("person", "age"), 30), CepEvent.append(List.of("tags"), "b")));
            log.append(List.of(CepEvent.set(List.of("person", "name"), "Alicia")));

            // Fold locally over the API (no extra methods required)
            Map<String,Object> state = new LinkedHashMap<>();
            for (CepEvent e : log.getAll()) state = e.fold(state);

            assertEquals("Alicia",        Paths.getObject(state, List.of("person","name")));
            assertEquals(30,              Paths.getObject(state, List.of("person","age")));
            assertEquals(List.of("a","b"),Paths.getObject(state, List.of("tags")));
        });
    }
}
