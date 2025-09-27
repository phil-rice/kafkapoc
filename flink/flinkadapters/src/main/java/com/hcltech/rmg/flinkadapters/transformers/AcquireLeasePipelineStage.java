package com.hcltech.rmg.flinkadapters.transformers;

import com.hcltech.rmg.cepstate.worklease.ITokenGenerator;
import com.hcltech.rmg.cepstate.worklease.WorkLease;
import com.hcltech.rmg.flinkadapters.envelopes.ValueRetryErrorEnvelope;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public class AcquireLeasePipelineStage extends RichFlatMapFunction<ValueRetryErrorEnvelope, ValueRetryErrorEnvelope> {

    private WorkLease<ValueRetryErrorEnvelope> workLease;


    @Override
    public void flatMap(ValueRetryErrorEnvelope valueRetryErrorEnvelope, Collector<ValueRetryErrorEnvelope> collector) throws Exception {
        String token = workLease.tryAcquire(valueRetryErrorEnvelope.domainId(), valueRetryErrorEnvelope);
        if (token != null)
            collector.collect(valueRetryErrorEnvelope.withToken(token));
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        this.workLease = WorkLease.memory(ITokenGenerator.generator());
    }
}
