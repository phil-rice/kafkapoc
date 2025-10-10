// src/test/java/com/hcltech/rmg/cepstate/InMemoryCepEventLogContractTest.java
package com.hcltech.rmg.cepstate;

import java.util.*;

public class InMemoryCepEventMapStringObjectLogTest implements AbstractCepEventLogContractTest<Map<String, Object>> {

    private final Map<String, List<CepEvent>> store = new HashMap<>();

    @Override
    public void withLog(String key, Body body) throws Exception {
        // simple per-key list
        List<CepEvent> buf = store.computeIfAbsent(key, k -> new ArrayList<>());
        CepEventLog log = new InMemoryCepEventLog<>();

        body.run(log);
    }

    @Override
    public CepStateTypeClass<Map<String, Object>> cepStateTypeClass() {
        return new MapStringObjectCepStateTypeClass();
    }

 }
