// src/test/java/com/hcltech/rmg/flinkadapters/FlinkCepEventLogContractTest.java
package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.cepstate.AbstractCepEventLogContractTest;
import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepEventLog;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class FlinkCepEventLogContractTest implements AbstractCepEventLogContractTest {

    @Override
    public void withLog(String key, Body body) throws Exception {
        // per-test per-key buffer (what the body sees)
        List<CepEvent> buf = new ArrayList<>();

        // operator: just materialize and append into buf (no outputs, no state)
        KeyedProcessFunction<String, CepEvent, Void> fn = new KeyedProcessFunction<>() {
            @Override
            public void open(OpenContext parameters) { /* no-op */ }

            @Override
            public void processElement(CepEvent e, Context ctx, Collector<Void> out) {
                if (e == null) return;
                synchronized (buf) {
                    buf.add(e);
                }
            }
        };

        var op = new KeyedProcessOperator<>(fn);
        try (var h = new KeyedOneInputStreamOperatorTestHarness<>(
                op,
                (KeySelector<CepEvent, String>) e -> key,
                TypeInformation.of(String.class))) {

            h.open();
            AtomicLong ts = new AtomicLong(0L);

            // Simple log impl, feeding the harness on append; reading from buf
            CepEventLog log = new CepEventLog() {
                @Override
                public void append(Collection<CepEvent> batch) {
                    if (batch == null || batch.isEmpty()) return;
                    for (CepEvent e : new ArrayList<>(batch)) { // defensive copy
                        try {
                            h.processElement(e, ts.getAndIncrement());
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }

                @Override
                public List<CepEvent> getAll() {
                    synchronized (buf) {
                        return new ArrayList<>(buf);
                    }
                }
            };

            body.run(log);
        }
    }
}
