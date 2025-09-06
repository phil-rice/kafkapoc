package com.hcltech.rmg.flinkadapters.transformers;

import com.hcltech.rmg.domainpipeline.TrainingRepository;
import com.hcltech.rmg.flinkadapters.envelopes.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

class FlinkPipelineLiftTest {

    // Side-output tags
    private static final OutputTag<RetryEnvelope<Object>> RETRIES =
            new OutputTag<>("retries", TypeInformation.of(new TypeHint<RetryEnvelope<Object>>() {
            })) {
            };
    private static final OutputTag<ErrorEnvelope<Object>> ERRORS =
            new OutputTag<>("errors", TypeInformation.of(new TypeHint<ErrorEnvelope<Object>>() {
            })) {
            };

    // DTO we store from each lane
    record TestOut(String id, String channel, String value) {
    }

    // In-memory sinks
    static final class MainSink implements SinkFunction<ValueEnvelope<Object>> {
        static final List<TestOut> OUT = new CopyOnWriteArrayList<>();

        @Override
        public void invoke(ValueEnvelope<Object> ve, Context ctx) {
            OUT.add(new TestOut(ve.domainId(), "main", String.valueOf(ve.data())));
        }
    }

    static final class ErrorSink implements SinkFunction<ErrorEnvelope<Object>> {
        static final List<TestOut> OUT = new CopyOnWriteArrayList<>();

        @Override
        public void invoke(ErrorEnvelope<Object> er, Context ctx) {
            String msg = er.errorMessages().isEmpty() ? "" : er.errorMessages().get(0);
            OUT.add(new TestOut(er.envelope().domainId(), "error", msg));
        }
    }

    static final class RetrySink implements SinkFunction<RetryEnvelope<Object>> {
        static final List<TestOut> OUT = new CopyOnWriteArrayList<>();

        @Override
        public void invoke(RetryEnvelope<Object> re, Context ctx) {
            OUT.add(new TestOut(re.envelope().domainId(), "retry", re.stageName()));
        }
    }

    @BeforeAll
    static void warmRepo() {
        String repoClass = TrainingRepository.class.getName();
        com.hcltech.rmg.interfaces.repository.IPipelineRepository.load(repoClass).pipelineDetails();
    }

    @BeforeEach
    void clearSinks() {
        MainSink.OUT.clear();
        ErrorSink.OUT.clear();
        RetrySink.OUT.clear();
    }

    @Test
    void e2e_multiScenario_collect_each_lane_assert_collections() throws Exception {
        String repoClass = TrainingRepository.class.getName();

        // Local bounded job
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(0);

        // IDs
        String okId = "ok-foo";
        String errId = "err-enrichment";
        String retryId = "rt-cepEnrichment";

        // Put control tokens in the PAYLOAD (StageFns.stage inspects payload), keep ids distinct
        ValueEnvelope<String> in1 = new ValueEnvelope<>("domainType1", okId, "foo", 100);
        ValueEnvelope<String> in2 = new ValueEnvelope<>("domainType2", errId, "Error:enrichment-data2", 200);
        ValueEnvelope<String> in3 = new ValueEnvelope<>("domainType3", retryId, "Retry:cepEnrichment-data3", 300);

        var src = env.fromElements(in1, in2, in3);

        // Lift
        var main = FlinkPipelineLift.lift(src, repoClass, RETRIES, ERRORS);

        // Sinks per lane
        main.addSink(new MainSink()).name("collect-main");
        main.getSideOutput(ERRORS).addSink(new ErrorSink()).name("collect-errors");
        main.getSideOutput(RETRIES).addSink(new RetrySink()).name("collect-retries");

        // Run once
        env.execute("e2e-multi");

        // Expected collections (sorted for deterministic assert)
        Comparator<TestOut> cmp = Comparator
                .comparing(TestOut::id)
                .thenComparing(TestOut::channel)
                .thenComparing(TestOut::value);

        List<TestOut> expectedMain = List.of(
                new TestOut(okId, "main", "bizlogic-enrichment-cep enrichment-validate-foo")
        );
        List<TestOut> expectedErrors = List.of(
                new TestOut(errId, "error", "enrichment: forced error")
        );
        List<TestOut> expectedRetries = List.of(
                new TestOut(retryId, "retry", "cepEnrichment")
        );

        // Actual (sorted)
        List<TestOut> gotMain = MainSink.OUT.stream().sorted(cmp).toList();
        List<TestOut> gotErrors = ErrorSink.OUT.stream().sorted(cmp).toList();
        List<TestOut> gotRetries = RetrySink.OUT.stream().sorted(cmp).toList();

        // Assert whole collections
        Assertions.assertEquals(expectedMain, gotMain, "main lane mismatch");
        Assertions.assertEquals(expectedErrors, gotErrors, "errors lane mismatch");
        Assertions.assertEquals(expectedRetries, gotRetries, "retries lane mismatch");
    }
}
