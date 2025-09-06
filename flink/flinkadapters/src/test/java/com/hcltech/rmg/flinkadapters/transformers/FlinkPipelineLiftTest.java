package com.hcltech.rmg.flinkadapters.transformers;

import com.hcltech.rmg.domainpipeline.TrainingRepository;
import com.hcltech.rmg.flinkadapters.envelopes.ErrorEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.RetryEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.ValueEnvelope;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class FlinkPipelineLiftTest {

    private final OutputTag<RetryEnvelope<Object>> RETRIES =
            new OutputTag<>("retries", TypeInformation.of(new TypeHint<RetryEnvelope<Object>>() {
            })) {
            };
    private final OutputTag<ErrorEnvelope<Object>> ERRORS =
            new OutputTag<>("errors", TypeInformation.of(new TypeHint<ErrorEnvelope<Object>>() {
            })) {
            };

    @Test
    void happyPath_trainingRepository() throws Exception {
        // 1) Local env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2) Input envelope
        // TrainingRepository.valueTC() maps domainId(value) = value, so payload "foo" => key "foo".
        // Adjust ctor to your real API if different.
        ValueEnvelope<String> in = new ValueEnvelope<>("domainType", "domainId", "foo", 1000); // data, domainId, stageName?
        DataStreamSource<ValueEnvelope<String>> source = env.fromElements(in);

        // 3) Side-output tags with explicit type info

        // 4) Lift using repository class name (no key selector needed anymore)
        String repoClass = TrainingRepository.class.getName();
        SingleOutputStreamOperator<ValueEnvelope<Object>> main =
                FlinkPipelineLift.lift(source, repoClass, RETRIES, ERRORS);

        // 5) Execute and collect main results
        List<String> got = new ArrayList<>();
        try (CloseableIterator<ValueEnvelope<Object>> it = main.executeAndCollect()) {
            while (it.hasNext()) got.add((String) it.next().data());
        }

        // 6) Assert (adjust expected if your stage ids/names differ)
        Assertions.assertEquals(
                List.of("bizlogic-enrichment-cep enrichment-validate-foo"),
                got
        );

        // (Optional) Side outputs should be empty on happy path
        try (CloseableIterator<ErrorEnvelope<Object>> errIt = main.getSideOutput(ERRORS).executeAndCollect()) {
            Assertions.assertFalse(errIt.hasNext());
        }
        try (CloseableIterator<RetryEnvelope<Object>> rtIt = main.getSideOutput(RETRIES).executeAndCollect()) {
            Assertions.assertFalse(rtIt.hasNext());
        }
    }
}
