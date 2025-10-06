// src/test/java/com/hcltech/rmg/cepstate/InMemoryCepEventLogContractTest.java
package com.hcltech.rmg.cepstate;

import java.util.*;

public class InMemoryCepEventLogTest implements AbstractCepEventLogContractTest {

    private final Map<String, List<CepEvent>> store = new HashMap<>();

    @Override
    public void withLog(String key, Body body) throws Exception {
        // simple per-key list
        List<CepEvent> buf = store.computeIfAbsent(key, k -> new ArrayList<>());

        // trivial CepEventLog over the buffer
        CepEventLog log = new CepEventLog() {
            @Override public void append(java.util.Collection<CepEvent> batch) {
                if (batch == null || batch.isEmpty()) return;
                buf.addAll(new ArrayList<>(batch));     // defensive copy
            }
            @Override public java.util.List<CepEvent> getAll() {
                return new ArrayList<>(buf);            // defensive copy to caller
            }
        };

        body.run(log);
    }
}
