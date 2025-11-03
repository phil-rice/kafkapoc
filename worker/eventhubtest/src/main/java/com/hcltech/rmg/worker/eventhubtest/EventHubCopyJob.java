package com.hcltech.rmg.worker.eventhubtest;

import com.hcltech.rmg.common.IEnvGetter;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import java.util.Properties;

public class EventHubCopyJob {

    // Single parallelism for ALL operators (source/map/sink). Matches your 32 partitions.
    private static final int PARALLELISM = 32;

    public static void main(String[] args) throws Exception {
        // Point JVM/Netty tmp at big disk (affects this JVM only; set TM dirs in cluster config later)
        final String tmpDir = "/datadrive/flink-tmp";
        System.setProperty("java.io.tmpdir", tmpDir);
        System.setProperty("io.netty.native.workdir", tmpDir);

        run(IEnvGetter.env);
    }

    /** Exposed for tests (inject an IEnvGetter that returns fake env vars). */
    public static void run(IEnvGetter env) throws Exception {
        KafkaDefaults defaults = new KafkaDefaults(
                "rmgeventhub-premium.servicebus.windows.net:9093", // Event Hubs bootstrap (Kafka endpoint)
                // "localhost:9092", // <- for local Kafka testing
                "testinputs",                               // must exist
                "processed",                                // must exist
                false                                       // false => ephemeral group by default
        );

        final KafkaConfig sourceCfg = CopyJobConfig.source(env, defaults);
        final KafkaConfig sinkCfg   = CopyJobConfig.sink(env, defaults);

        // ---- Local mini-cluster with explicit network memory + slots ----
        Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, PARALLELISM);
        // Give the shuffle/network buffer pool real headroom (default ~64MB is too small for 32-way pipelines)
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("512 mb"));
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse("512 mb"));
        // Optional: larger segments reduce fragmentation a bit; leave default unless needed
        // conf.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("64 kb"));

        final StreamExecutionEnvironment see = StreamExecutionEnvironment.createLocalEnvironment(conf);
        // Same parallelism for all operators; we don't do keyBy/shuffle, so edges are FORWARD (no reshuffle)
        see.setParallelism(PARALLELISM);

        // Build Kafka source/sink
        final KafkaSource<String> source = buildSource(sourceCfg, env);
        final KafkaSink<String>   sink   = buildSink(sinkCfg, env);

        // Pipeline: source -> map -> sink
        // - Same parallelism throughout (inherits from env)
        // - No repartition ops => one-to-one edges, typically chained (no network reshuffle)
        see.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
                .name("source")
                .slotSharingGroup("all")
                .setParallelism(12)
                .map(new LogWhatWeHaveFunction())
                .name("map")
                .slotSharingGroup("all")
                .sinkTo(sink)
                .name("sink: Writer")
                .slotSharingGroup("all");

        see.execute("EventHub Copy Job");
    }

    // --------- Build Source/Sink ---------

    private static KafkaSource<String> buildSource(KafkaConfig cfg, IEnvGetter env) {
        KafkaSourceBuilder<String> b = KafkaSource.<String>builder()
                .setBootstrapServers(cfg.bootstrapServer())
                .setTopics(cfg.topic())
                .setGroupId(cfg.groupId())
                .setValueOnlyDeserializer(new SimpleStringSchema());

        // Starting offsets
        OffsetsInitializer init = "earliest".equalsIgnoreCase(cfg.startingOffsets())
                ? OffsetsInitializer.earliest()
                : OffsetsInitializer.latest();
        b.setStartingOffsets(init);

        // Throughput-oriented consumer tuning (good defaults over TLS/WAN)
        b.setProperty("fetch.min.bytes", "1048576");            // ~1 MiB aggregate per fetch
        b.setProperty("fetch.max.wait.ms", "50");               // wait up to 50ms to fill batch
        b.setProperty("max.partition.fetch.bytes", "1048576");  // ~1 MiB per partition per fetch
        b.setProperty("max.poll.records", "1000");              // deliver larger batches to operator

        // Partition discovery interval (Kafka consumer property)
        if (cfg.partitionDiscovery() != null) {
            long ms = cfg.partitionDiscovery().toMillis();
            b.setProperty("partition.discovery.interval.ms", Long.toString(ms));
        }

        // Extra consumer props
        Properties consumerProps = copy(cfg.properties());
        // Event Hubs auth (Kafka-compatible SASL/SSL)
        if (cfg.eventHub()) {
            addEventHubSaslProps(consumerProps, env);
        }
        consumerProps.forEach((k, v) -> b.setProperty(k.toString(), v.toString()));

        return b.build();
    }

    private static KafkaSink<String> buildSink(KafkaConfig cfg, IEnvGetter env) {
        Properties producerProps = copy(cfg.properties());
        if (cfg.eventHub()) {
            addEventHubSaslProps(producerProps, env);
        }

        // Throughput-friendly producer batching (compression left OFF for now)
        producerProps.putIfAbsent("linger.ms", "50");                 // accumulate ~50ms to batch
        producerProps.putIfAbsent("batch.size", "262144");            // 256 KiB per-partition batch
        // producerProps.putIfAbsent("compression.type", "lz4");      // enable later if desired

        // Reasonable reliability without over-amping latency
        producerProps.putIfAbsent("acks", "1");
        producerProps.putIfAbsent("max.in.flight.requests.per.connection", "5");
        producerProps.putIfAbsent("retries", "10");
        producerProps.putIfAbsent("delivery.timeout.ms", "120000");
        producerProps.putIfAbsent("request.timeout.ms", "60000");

        // Trim buffer to reduce GC spikes when running 32 producers in one JVM
        producerProps.putIfAbsent("buffer.memory", "33554432");       // 32 MiB

        return KafkaSink.<String>builder()
                .setBootstrapServers(cfg.bootstrapServer())
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic(cfg.topic())
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setKafkaProducerConfig(producerProps)
                .build();
    }

    private static void addEventHubSaslProps(Properties p, IEnvGetter env) {
        String cs = IEnvGetter.getString(env, "EVENTHUB_CONNECTION_STRING");
        p.setProperty("security.protocol", "SASL_SSL");
        p.setProperty("sasl.mechanism", "PLAIN");
        p.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"$ConnectionString\" "
                        + "password=\"" + cs + "\";");
    }

    private static Properties copy(Properties in) {
        Properties out = new Properties();
        if (in != null) {
            in.forEach((k, v) -> out.put(k, v));
        }
        return out;
    }
}
