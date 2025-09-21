package com.hcltech.rmg.flinkadapters.kafka;

import com.hcltech.rmg.common.TestDomainTracker;

public class ToEnvelopeMapForTestDomainTracker extends ToEnvelopeMap<TestDomainTracker> {


    public ToEnvelopeMapForTestDomainTracker(String domainType, String clazzName, int lanes) {
        super(domainType, clazzName, lanes);
    }


    @Override
    String domainIdExtractor(TestDomainTracker t) {
        return t.domainId();
    }

    @Override
    TestDomainTracker withInitialValues(TestDomainTracker testDomainTracker) {
        return testDomainTracker.withStart(System.currentTimeMillis());
    }

}
