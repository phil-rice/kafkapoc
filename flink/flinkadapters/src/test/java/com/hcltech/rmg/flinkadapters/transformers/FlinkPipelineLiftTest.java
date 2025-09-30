// src/test/java/com/hcltech/rmg/flinkadapters/transformers/FlinkPipelineLiftTest.java
package com.hcltech.rmg.flinkadapters.transformers;

import com.hcltech.rmg.domainpipeline.TrainingRepository;
import com.hcltech.rmg.flinkadapters.envelopes.ErrorEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.RetryEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.ValueEnvelope;
import com.hcltech.rmg.flinkadapters.kafka.RawKafkaData;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.*;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

class FlinkPipelineLiftTest {

    private static final OutputTag<RetryEnvelope<Object>> RETRIES =
            new OutputTag<>("retries", TypeInformation.of(new TypeHint<RetryEnvelope<Object>>(){})){};
    private static final OutputTag<ErrorEnvelope<Object>> ERRORS =
            new OutputTag<>("errors", TypeInformation.of(new TypeHint<ErrorEnvelope<Object>>(){})){};

    record TestOut(String id, String channel, String value) {}

    static final class MainSink implements SinkFunction<ValueEnvelope<Object>> {
        static final List<TestOut> OUT = new CopyOnWriteArrayList<>();
        @Override public void invoke(ValueEnvelope<Object> ve, Context ctx) {
            OUT.add(new TestOut(ve.domainId(), "main", String.valueOf(ve.data())));
        }
    }

    static final class ErrorSink implements SinkFunction<ErrorEnvelope<Object>> {
        static final List<TestOut> OUT = new CopyOnWriteArrayList<>();
        @Override public void invoke(ErrorEnvelope<Object> er, Context ctx) {
            String msg = er.errorMessages().isEmpty() ? "" : er.errorMessages().get(0);
            OUT.add(new TestOut(er.envelope().domainId(), "error", msg));
        }
    }

    static final class RetrySink implements SinkFunction<RetryEnvelope<Object>> {
        static final List<TestOut> OUT = new CopyOnWriteArrayList<>();
        @Override public void invoke(RetryEnvelope<Object> re, Context ctx) {
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
    void e2e_sync_and_async_value_error_retry() throws Exception {
        String repoClass = TrainingRepository.class.getName();

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(0);

        // Distinct ids; control tokens live in PAYLOAD
        String okId = "ok-foo";
        String errEnrich = "err-enrichment";     // async error
        String rtCep = "rt-cepEnrichment";       // sync retry
        String errValid = "err-validate";        // sync error
        String rtBiz = "rt-bizLogic";            // async retry

        RawKafkaData rkd1 = new RawKafkaData("foo", "key-1", 2, 1001L, 1693651200000L);
        ValueEnvelope<String> in1 = new ValueEnvelope<>("domainType1", okId, null, "foo", 100, rkd1);

        RawKafkaData rkd2 = new RawKafkaData("err-enrichment", "key-2", 2, 1002L, 1693651200000L);
        ValueEnvelope<String> in2 = new ValueEnvelope<>("domainType2", errEnrich, null, "Error:enrichment-data2", 200, rkd2);

        RawKafkaData rkd3 = new RawKafkaData("rt-cepEnrichment", "key-3", 2, 1003L, 1693651200000L);
        ValueEnvelope<String> in3 = new ValueEnvelope<>("domainType3", rtCep, null, "Retry:cepEnrichment-data3", 300, rkd3);

        RawKafkaData rkd4 = new RawKafkaData("err-validate", "key-4", 2, 1004L, 1693651200000L);
        ValueEnvelope<String> in4 = new ValueEnvelope<>("domainType4", errValid, null, "Error:validate-data4", 400, rkd4);

        RawKafkaData rkd5 = new RawKafkaData("rt-bizLogic", "key-5", 2, 1005L, 1693651200000L);
        ValueEnvelope<String> in5 = new ValueEnvelope<>("domainType5", rtBiz, null, "Retry:bizLogic-data5", 500, rkd5);

        var src = env.fromElements(in1, in2, in3, in4, in5);

        // lanes=10, maxRetries=3 (arbitrary for test), orderedAsync=false, timeoutBufferMs=10_000
        var main = FlinkPipelineLift.lift(src, repoClass, RETRIES, ERRORS, 10, 3, false, 10_000);

        main.addSink(new MainSink()).name("collect-main");
        main.getSideOutput(ERRORS).addSink(new ErrorSink()).name("collect-errors");
        main.getSideOutput(RETRIES).addSink(new RetrySink()).name("collect-retries");

        env.execute("e2e-sync-async");

        // Comparator for deterministic equality (if multiple in a lane)
        Comparator<TestOut> cmp = Comparator.comparing(TestOut::id)
                .thenComparing(TestOut::channel)
                .thenComparing(TestOut::value);

        // Expected
        List<TestOut> expectedMain = List.of(
                new TestOut(okId, "main", "bizlogic-enrichment-cep enrichment-validate-foo")
        );
        List<TestOut> expectedErrors = List.of(
                new TestOut(errEnrich, "error", "enrichment: forced error"), // async stage error
                new TestOut(errValid, "error", "validate: forced error")    // sync stage error
        );
        List<TestOut> expectedRetries = List.of(
                new TestOut(rtCep, "retry", "cepEnrichment"), // sync stage retry
                new TestOut(rtBiz, "retry", "bizLogic")       // async stage retry
        );

        // Actual (sorted)
        List<TestOut> gotMain = MainSink.OUT.stream().sorted(cmp).toList();
        List<TestOut> gotErrors = ErrorSink.OUT.stream().sorted(cmp).toList();
        List<TestOut> gotRetries = RetrySink.OUT.stream().sorted(cmp).toList();

        Assertions.assertEquals(expectedMain.stream().sorted(cmp).toList(), gotMain, "main lane mismatch");
        Assertions.assertEquals(expectedErrors.stream().sorted(cmp).toList(), gotErrors, "errors lane mismatch");
        Assertions.assertEquals(expectedRetries.stream().sorted(cmp).toList(), gotRetries, "retries lane mismatch");
    }
}
