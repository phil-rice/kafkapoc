package com.hcltech.rmg.worker.eventhubtest;

import com.hcltech.rmg.common.IEnvGetter;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class EventHubCopyJob {

    public static void main(String[] args) throws Exception {
        run(IEnvGetter.env);
    }

    /**
     * Exposed for tests (inject an IEnvGetter that returns fake env vars).
     */
    public static void run(IEnvGetter env) throws Exception {
        KafkaDefaults defaults = new KafkaDefaults(
                "rmgeventhub.servicebus.windows.net:9093", // default Event Hub bootstrap
//                "localhost:9092", replace the above with this to test against local Kafka
                "mper-input-events",   // must match an existing Event Hub (source)
                "processed",   // must match an existing Event Hub (sink)
                false//new group id each run
        );


        final KafkaConfig sourceCfg = CopyJobConfig.source(env, defaults);
        final KafkaConfig sinkCfg = CopyJobConfig.sink(env, defaults);

        final StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        final KafkaSource<String> source = buildSource(sourceCfg, env);
        final KafkaSink<String> sink = buildSink(sinkCfg, env);

        if (sourceCfg.sourceParallelism() > 0) {
            see.setParallelism(sourceCfg.sourceParallelism());
        }

        see.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
                .map(new LogWhatWeHaveFunction())
                .sinkTo(sink)
                .name("sink");

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

        // Partition discovery interval (Kafka consumer property) — direct set
        if (cfg.partitionDiscovery() != null) {
            long ms = cfg.partitionDiscovery().toMillis();
            b.setProperty("partition.discovery.interval.ms", Long.toString(ms));
        }

        // Extra consumer props
        Properties consumerProps = copy(cfg.extra());
        // Event Hubs auth (via Kafka-compatible SASL/SSL)
        if (cfg.eventHub()) {
            addEventHubSaslProps(consumerProps, env);
        }
        consumerProps.forEach((k, v) -> b.setProperty(k.toString(), v.toString()));

        return b.build();
    }

    private static KafkaSink<String> buildSink(KafkaConfig cfg, IEnvGetter env) {
        Properties producerProps = copy(cfg.extra());
        if (cfg.eventHub()) {
            addEventHubSaslProps(producerProps, env);
        }

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
