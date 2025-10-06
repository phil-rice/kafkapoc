package com.hcltech.rmg.cepstate;

public class InMemoryCepEventLogTest implements AbstractCepEventLogContractTest {
    @Override
    public CepEventLog newLog() {
        return new InMemoryCepEventLog();
    }
}
