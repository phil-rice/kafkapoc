package com.hcltech.rmg.cepstate;

import com.hcltech.rmg.common.Paths;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FlinkCepEventLogOpTest {

    private KeyedOneInputStreamOperatorTestHarness<String, CepEventLogOp.In, CepEventLogOp.Out> h;

    /** Serializable KeySelector for inputs (avoids NotSerializableException). */
    public static final class InKeySelector implements KeySelector<CepEventLogOp.In, String>, Serializable {
        @Override
        public String getKey(CepEventLogOp.In in) {
            if (in instanceof CepEventLogOp.Append a) return a.domainId();
            if (in instanceof CepEventLogOp.QueryAll q) return q.domainId();
            if (in instanceof CepEventLogOp.QueryFold qf) return qf.domainId();
            return "UNKNOWN";
        }
    }

    @BeforeEach
    void setup() throws Exception {
        var op = new StreamFlatMap<>(new CepEventLogOp());
        h = new KeyedOneInputStreamOperatorTestHarness<>(
                op,
                new InKeySelector(),   // <-- serializable
                Types.STRING
        );
        h.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (h != null) h.close();
    }

    @Test
    void perKey_append_and_queryAll_preserve_order_and_isolate_by_key() throws Exception {
        // A: two batches
        h.processElement(new CepEventLogOp.Append("A", List.of(
                CepEvent.set(List.of("a"), 1),
                CepEvent.append(List.of("tags"), "x")
        )), 0);
        h.processElement(new CepEventLogOp.Append("A", List.of(
                CepEvent.set(List.of("b"), true)
        )), 1);

        // B: one batch
        h.processElement(new CepEventLogOp.Append("B", List.of(
                CepEvent.append(List.of("tags"), "y")
        )), 2);

        // Query A
        h.processElement(new CepEventLogOp.QueryAll("A"), 3);
        var outA = drain(h);
        assertTrue(outA.get(outA.size()-1) instanceof CepEventLogOp.AllEvents);
        var eventsA = ((CepEventLogOp.AllEvents) outA.get(outA.size()-1)).events();
        assertEquals(3, eventsA.size());
        assertEquals(List.of(
                CepEvent.set(List.of("a"), 1),
                CepEvent.append(List.of("tags"), "x"),
                CepEvent.set(List.of("b"), true)
        ), eventsA);

        // Query B
        h.processElement(new CepEventLogOp.QueryAll("B"), 4);
        var outB = drain(h);
        assertTrue(outB.get(outB.size()-1) instanceof CepEventLogOp.AllEvents);
        var eventsB = ((CepEventLogOp.AllEvents) outB.get(outB.size()-1)).events();
        assertEquals(1, eventsB.size());
        assertEquals(CepEvent.append(List.of("tags"), "y"), eventsB.get(0));
    }

    @Test
    void queryFold_materializes_expected_state() throws Exception {
        h.processElement(new CepEventLogOp.Append("K", List.of(
                CepEvent.set(List.of("person","name"), "Alice"),
                CepEvent.append(List.of("tags"), "a")
        )), 0);

        h.processElement(new CepEventLogOp.Append("K", List.of(
                CepEvent.set(List.of("person","age"), 30),
                CepEvent.append(List.of("tags"), "b")
        )), 1);

        h.processElement(new CepEventLogOp.Append("K", List.of(
                CepEvent.set(List.of("person","name"), "Alicia")
        )), 2);

        h.processElement(new CepEventLogOp.QueryFold("K"), 3);
        var out = drain(h);
        assertTrue(out.get(out.size()-1) instanceof CepEventLogOp.FoldedState);
        Map<String,Object> state = ((CepEventLogOp.FoldedState) out.get(out.size()-1)).state();

        assertEquals("Alicia", Paths.getObject(state, List.of("person","name")));
        assertEquals(30, Paths.getObject(state, List.of("person","age")));
        assertEquals(List.of("a","b"), Paths.getObject(state, List.of("tags")));
    }


    /** Drain only StreamRecord<Out> values from the harness output. */
    private static List<CepEventLogOp.Out> drain(
            KeyedOneInputStreamOperatorTestHarness<String, CepEventLogOp.In, CepEventLogOp.Out> h) {
        var out = new ArrayList<CepEventLogOp.Out>();
        Object el;
        while ((el = h.getOutput().poll()) != null) {
            if (el instanceof StreamRecord<?> rec) {
                Object v = rec.getValue();
                if (v instanceof CepEventLogOp.Out o) out.add(o);
            }
        }
        return out;
    }
}
