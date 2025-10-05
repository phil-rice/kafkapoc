package com.hcltech.rmg.performance;

import com.hcltech.rmg.appcontainer.impl.AppContainer;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainer;
import com.hcltech.rmg.common.TestDomainTracker;
import com.hcltech.rmg.domainpipeline.TestDomainRepository;
import com.hcltech.rmg.flinkadapters.xml.KeySniffAndClassify;
import com.hcltech.rmg.interfaces.repository.IPipelineRepository;
import com.hcltech.rmg.kafka.KafkaSourceForFlink;
import com.hcltech.rmg.kafka.WatermarkStrategyProvider;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import com.hcltech.rmg.messages.ErrorEnvelope;
import com.hcltech.rmg.messages.RawMessage;
import com.hcltech.rmg.messages.RetryEnvelope;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

public final class PerfHarnessMain {
    public static <CEPState> DataStream<Tuple2<String, RawMessage>> buildPipeline(int lanes) {

        final String repoClass = TestDomainRepository.class.getName();
        final String containerId = System.getProperty("app.container", "prod");
        IAppContainer<KafkaConfig> appContainer = AppContainer.resolve(containerId);
        final KafkaConfig kafkaConfig = appContainer.eventSourceConfig();
        final String xmlSchemaName = appContainer.rootConfig().xmlSchemaPath();

        // ---- config via -D or defaults ----
        final String bootstrap = kafkaConfig.bootstrapServers();
        final String topic = kafkaConfig.topic();
        final String groupId = kafkaConfig.groupId();
        final int partitions = kafkaConfig.sourceParallelism();

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(partitions > 0 ? partitions : env.getParallelism());
        env.getConfig().setAutoWatermarkInterval(0);
        env.enableCheckpointing(30_000);
        // For the perf harness this is fine and reduces GC:
        // env.getConfig().enableObjectReuse();

        // ---- source: RawMessage via our helper ----
        var raw = KafkaSourceForFlink.rawKafkaStream(
                containerId, env, bootstrap, topic, groupId,
                partitions, OffsetsInitializer.earliest(),
                Duration.ofSeconds(60), WatermarkStrategyProvider.none());

        // ---- side-output tags (make payload concrete to avoid generic Kryo) ----
        OutputTag<RetryEnvelope<CEPState, RawMessage>> RETRIES =
                new OutputTag<>("retries",
                        TypeInformation.of(new TypeHint<RetryEnvelope<CEPState, RawMessage>>() {
                        })) {
                };

        OutputTag<ErrorEnvelope<CEPState, RawMessage>> ERRORS =
                new OutputTag<>("errors",
                        TypeInformation.of(new TypeHint<ErrorEnvelope<CEPState, RawMessage>>() {
                        })) {
                };

        // ---- pre-shuffle: sniff key -> (domainId, raw) ; errors to side-output ----
        var withKey = raw
                .process(new KeySniffAndClassify<CEPState>(containerId, ERRORS, lanes))
                .name("key-sniff"); // DataStream<Tuple2<String, RawMessage>>

        // (Optional: observe sniff errors here)
        // withKey.getSideOutput(ERRORS).addSink(...);

        return withKey;
    }

    public static void main(String[] args) throws Exception {
        var lanes = 1200;
        IPipelineRepository.load(TestDomainRepository.class.getName()).pipelineDetails();

        var rawMessageAndIdStream = buildPipeline(lanes);
        var keyed = rawMessageAndIdStream.keyBy(t -> t.f0); // domainId
        var envelopes = keyed.map()
                .name("strip-key");

        // ---- source: RawMessage via our new helper ----

        // ---- decode → ValueEnvelope<String> (domainId == payload here; adapt if needed) ----
        // ToEnvelopeMapForTestDomainTracker should accept RawMessage and pull raw.rawValue().
        var envelopes = raw
                .map(new ToEnvelopeMapForTestDomainTracker(
                        "domainType",
                        TestDomainTracker.class.getName(),
                        lanes
                ))
                .name("to-envelope");

        // ---- side-output tags (errors/retries) ----

        // ---- lift (non-keyed lift that splits once at end) ----
        int timeOutBufferMs = 2000;
        int maxRetries = 3;
        var main = FlinkPipelineLift.lift(
                envelopes,
                repoClass,
                RETRIES,
                ERRORS,
                lanes,
                maxRetries,
                false,              // whatever your existing flag means
                timeOutBufferMs
        );

        // ---- progress sinks (prints every N items per lane) ----
        main.addSink(new Every<>("main", lanes * 20)).name("main-counter");
        main.getSideOutput(ERRORS).addSink(new Every<>("error", 50)).name("error-counter");
        main.getSideOutput(RETRIES).addSink(new Every<>("retry", 100)).name("retry-counter");

        env.execute("rmg-perf-harness");
    }

    /**
     * Tiny sink that logs count every N records.
     */
    static final class Every<T> extends RichSinkFunction<T> implements java.io.Serializable {
        private final String lane;
        private final long every;
        private static final AtomicLong c = new AtomicLong(0);
        private transient TaskInfo taskInfo;
        private static long start = System.currentTimeMillis();
        private static final long initialStart = start;
        private static long lastCount = 0;
        private static final Object lock = new Object();

        Every(String lane, long every) {
            this.lane = lane;
            this.every = every;
        }

        @Override
        public void open(OpenContext context) {
            var ctx = getRuntimeContext();
            this.taskInfo = ctx.getTaskInfo();
        }

        @Override
        public void invoke(T value, Context context) {
            if (c.incrementAndGet() % every == 0) {
                synchronized (lock) {
                    if (c.get() % every != 0) return;
                    var now = System.currentTimeMillis();
                    var duration = now - start;
                    var localPerS = (c.get() - lastCount) * 1000f / duration;
                    var fullPerS = c.get() * 1000f / (now - initialStart);
                    lastCount = c.get();
                    start = now;
                    System.out.println("[" + lane + "] processed=" + c +
                            " perSec=" + localPerS + "/" + fullPerS +
                            " task=" + taskInfo.getIndexOfThisSubtask() + " --- " + taskInfo.getTaskName() +
                            " thread=" + Thread.currentThread().getName());
                }
            }
        }
    }
}
