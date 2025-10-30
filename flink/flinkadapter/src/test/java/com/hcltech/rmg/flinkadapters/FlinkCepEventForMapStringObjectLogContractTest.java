package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.cepstate.*;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import java.util.Map;

public class FlinkCepEventForMapStringObjectLogContractTest
        implements AbstractCepEventLogContractTest<Map<String, Object>> {

    @Override
    public void withLog(String key, Body body) throws Exception {

        KeyedProcessFunction<String, Integer, Void> fn = new KeyedProcessFunction<>() {
            private transient CepEventLog log;
            private boolean ran = false;

            @Override
            public void open(OpenContext parameters) {
                // Build the log once we have a runtime context
                log = FlinkCepEventForMapStringObjectLog.from(getRuntimeContext(), "cepState");
            }

            @Override
            public void processElement(Integer ignored, Context ctx, Collector<Void> out) throws Exception {
                // We are now in keyed context (currentKey == key). Run the test body here.
                if (!ran) {
                    ran = true;
                    body.run(log);
                }
            }
        };

        var op = new KeyedProcessOperator<>(fn);

        try (var h = new KeyedOneInputStreamOperatorTestHarness<>(
                op,
                (KeySelector<Integer, String>) in -> key,        // everything mapped to the supplied key
                TypeInformation.of(String.class))) {

            h.open();

            // Push a single dummy element to establish key context and trigger body.run(log)
            h.processElement(new StreamRecord<>(0));
        }
    }

    @Override
    public CepStateTypeClass<Map<String, Object>> cepStateTypeClass() {
        return new MapStringObjectCepStateTypeClass();
    }
}
