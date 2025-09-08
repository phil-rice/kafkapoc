package com.hcltech.rmg.flinkadapters.kafka;

import com.hcltech.rmg.common.TestDomainTracker;
import com.hcltech.rmg.common.codec.Codec;
import com.hcltech.rmg.flinkadapters.envelopes.ValueEnvelope;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.function.Function;

public class ToEnvelopeMapForTestDomainTracker extends ToEnvelopeMap<TestDomainTracker> {


    public ToEnvelopeMapForTestDomainTracker(String domainType, String clazzName, int lanes) {
        super(domainType, clazzName, lanes);
    }

    @Override
    String domainIdExtractor(TestDomainTracker t) {
        return t.domainId();
    }

}
