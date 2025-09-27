package com.hcltech.rmg.performance;

import com.hcltech.rmg.common.TestDomainTracker;
import com.hcltech.rmg.domainpipeline.TestDomainRepository;
import com.hcltech.rmg.flinkadapters.envelopes.ErrorEnvelope;
import com.hcltech.rmg.flinkadapters.envelopes.RetryEnvelope;
import com.hcltech.rmg.flinkadapters.kafka.RawKafkaData;
import com.hcltech.rmg.flinkadapters.kafka.RawKafkaDataDeserializer;
import com.hcltech.rmg.flinkadapters.kafka.ToEnvelopeMapForTestDomainTracker;
import com.hcltech.rmg.flinkadapters.transformers.FlinkPipelineLift;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.atomic.AtomicLong;

public final class PerfHarnessMain {

    public static void main(String[] args) throws Exception {
        final String repoClass = TestDomainRepository.class.getName();

        // ---- config via -D or defaults ----
        final String bootstrap = System.getProperty("kafka.bootstrap", "localhost:9092");
        final String topic = "test-topic";
        final String groupId = "test-perf-" + System.currentTimeMillis();
        final int partitions = Integer.getInteger("kafka.partitions", 12); // pass if you want 1:1 mapping
        final int lanes = 200; //per partition


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(partitions > 0 ? partitions : env.getParallelism());
        env.getConfig().setAutoWatermarkInterval(0);
        env.enableCheckpointing(30_000); // at-least-once offsets commits
        // (Optional) tune for perf:
        // env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000);
        // env.getConfig().enableObjectReuse();

        // warm repo (class load upfront)
        com.hcltech.rmg.interfaces.repository.IPipelineRepository.load(repoClass).pipelineDetails();

        // ---- kafka source (value-only strings) ----
        KafkaSource<RawKafkaData> source = KafkaSource.<RawKafkaData>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(groupId)
                .setDeserializer(new RawKafkaDataDeserializer())
                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                // partition discovery optional (for growing topics):
                .setProperty("partition.discovery.interval.ms", "60000")
                .build();

        DataStreamSource<RawKafkaData> raw = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-" + topic);

        if (partitions > 0)
            raw.setParallelism(partitions);

        // ---- decode -> ValueEnvelope<String>  (domainId == payload here; adapt if different) ----
        var envelopes = raw
                .map(new ToEnvelopeMapForTestDomainTracker("domainType", TestDomainTracker.class.getName(), lanes))
                .name("to-envelope");

        // ---- side-output tags (errors/retries) ----
        OutputTag<RetryEnvelope<Object>> RETRIES = new OutputTag<>("retries",
                TypeInformation.of(new TypeHint<RetryEnvelope<Object>>() {
                })) {
        };
        OutputTag<ErrorEnvelope<Object>> ERRORS = new OutputTag<>("errors",
                TypeInformation.of(new TypeHint<ErrorEnvelope<Object>>() {
                })) {
        };

        // ---- lift (your non-keyed lift that splits once at end) ----
        // NOTE: per-domain order is preserved because:
        //  - Kafka preserves per-partition order
        //  - We keep one subtask per partition (no keyBy shuffle)
        //  - We’ll use orderedWait inside the lift for async stages (per-subtask order)
        int timeOutBufferMs = 2000;
        int maxRetries = 3;
        var main = FlinkPipelineLift.lift(envelopes, repoClass, RETRIES, ERRORS, lanes, maxRetries, false, timeOutBufferMs);

        // ---- progress sinks (prints every N items per lane) ----
        main.addSink(new Every<>("main", lanes * 20)).name("main-counter");
        main.getSideOutput(ERRORS).addSink(new Every<>("error", 50)).name("error-counter");
        main.getSideOutput(RETRIES).addSink(new Every<>("retry", 100)).name("retry-counter");

        // Tto write to Kafka, add sinks here using  JSON codecs.
        // Note: the code is not executed here. The pipeline is copied to the workers...
        env.execute("rmg-perf-harness");
    }

    /**
     * Tiny sink that logs count every N records.
     */
    static final class Every<T> extends RichSinkFunction<T> implements java.io.Serializable {
        private final String lane;
        private final long every;
        private final static AtomicLong c = new AtomicLong(0);
        private transient TaskInfo taskInfo;
        private static long start = System.currentTimeMillis();
        private static final long initialStart = start;
        private static long lastCount = 0;
        private static Object lock = new Object();

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
                    if (c.get() % every != 0) {
                        return;
                    }
                    var now = System.currentTimeMillis();
                    var duration = now - start;
                    var localPerS = (c.get() - lastCount) * 1000f / duration;
                    var fullPerS = c.get() * 1000f / (now - initialStart);
                    lastCount = c.get();
                    start = now;
                    System.out.println("[" + lane + "] processed=" + c +
                            " perSec=" + localPerS + "/" + fullPerS +
                            " task=" + taskInfo.getIndexOfThisSubtask() + " --- " + taskInfo.getTaskName() + " thread=" + Thread.currentThread().getName());
                }
            }
        }
    }
}
