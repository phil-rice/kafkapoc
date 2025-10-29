package com.hcltech.rmg.shared_worker;

import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.kafka.KafkaSourceForFlink;
import com.hcltech.rmg.kafka.RawMessageDeserialiser;
import com.hcltech.rmg.kafka.WatermarkStrategyProvider;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import com.hcltech.rmg.messages.RawMessage;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Properties;

public class KafkaFlinkHelper {
    static public <ESC, CepState, Msg, Schema, RT, FR, MetricsParam> DataStreamSource<RawMessage> createRawMessageStreamFromKafka(AppContainerDefn<ESC, CepState, Msg, Schema, RT, FR, MetricsParam> appContainerDefn, StreamExecutionEnvironment env, KafkaConfig kafka, int checkpointingInterval) {
        if (kafka.eventHub())
            return createRawMessageStreamFromEventHub(appContainerDefn, env, kafka, checkpointingInterval);
        else
            return createRawMessageStreamFromKafkaNotEventHub(appContainerDefn, env, kafka, checkpointingInterval);
    }

    static public <ESC, CepState, Msg, Schema, RT, FR, MetricsParam> DataStreamSource<RawMessage> createRawMessageStreamFromKafkaNotEventHub(AppContainerDefn<ESC, CepState, Msg, Schema, RT, FR, MetricsParam> appContainerDefn, StreamExecutionEnvironment env, KafkaConfig kafka, int checkpointingInterval) {
        final int totalPartitions = kafka.sourceParallelism();
        env.setParallelism(totalPartitions > 0 ? totalPartitions : env.getParallelism());
        env.getConfig().setAutoWatermarkInterval(0);
        env.enableCheckpointing(checkpointingInterval);
        try {
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure RocksDB state backend", e);
        }

        var raw = KafkaSourceForFlink.rawKafkaStream(appContainerDefn, env, kafka.bootstrapServer(), kafka.topic(), kafka.groupId(), totalPartitions, OffsetsInitializer.earliest(), Duration.ofSeconds(60), WatermarkStrategyProvider.none());
        return raw;
    }

    static public <ESC, CepState, Msg, Schema, RT, FR, MetricsParam> DataStreamSource<RawMessage> createRawMessageStreamFromEventHub(AppContainerDefn<ESC, CepState, Msg, Schema, RT, FR, MetricsParam> appContainerDefn, StreamExecutionEnvironment env, KafkaConfig kafka, int checkpointingInterval) {
//        Properties extra = new Properties();
//        extra.setProperty("security.protocol", "SASL_SSL");
//        extra.setProperty("sasl.mechanism", "PLAIN");
//        extra.setProperty(
//                "sasl.jaas.config",
//                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
//                        "username=\"$ConnectionString\" " +
//                        "password=\"Endpoint=sb://<NAMESPACE>.servicebus.windows.net/;SharedAccessKeyName=<POLICY>;SharedAccessKey=<KEY>\";"
//        );
//        extra.setProperty("ssl.endpoint.identification.algorithm", "https"); // SNI/hostname verification
//
//        KafkaSource<RawMessage> source =
//                KafkaSource.<RawMessage>builder()
//                        .setBootstrapServers("<NAMESPACE>.servicebus.windows.net:9093")
//                        .setTopics("<EVENT_HUB_NAME>")
//                        .setGroupId("<CONSUMER_GROUP>")      // e.g. $Default
//                        .setDeserializer(new RawMessageDeserialiser(appContainerDefn))
//                        .setStartingOffsets(startingOffsets)
//                        .setProperties(extra)
//                        .build();

        final int totalPartitions = kafka.sourceParallelism();
        env.setParallelism(totalPartitions > 0 ? totalPartitions : env.getParallelism());
        env.getConfig().setAutoWatermarkInterval(0);
        env.enableCheckpointing(checkpointingInterval);
        try {
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure RocksDB state backend", e);
        }

        String topic = kafka.topic();
        Duration partitionDiscoveryInterval = Duration.ofSeconds(60);
        WatermarkStrategyProvider<RawMessage> wmProvider = WatermarkStrategyProvider.none();
        Properties extra = new Properties();
        if (partitionDiscoveryInterval != null) {
            extra.put("partition.discovery.interval.ms", String.valueOf(partitionDiscoveryInterval.toMillis()));
        }
        extra.setProperty("security.protocol", "SASL_SSL");
        extra.setProperty("sasl.mechanism", "PLAIN");
        extra.setProperty(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"$ConnectionString\" " +
                        "password=\"Endpoint=sb://<NAMESPACE>.servicebus.windows.net/;SharedAccessKeyName=<POLICY>;SharedAccessKey=<KEY>\";"
        );
        extra.setProperty("ssl.endpoint.identification.algorithm", "https"); // SNI/hostname verification

        KafkaSource<RawMessage> source = KafkaSource.<RawMessage>builder()
                .setBootstrapServers(kafka.bootstrapServer())
                .setTopics(topic).setGroupId(kafka.groupId())
                .setDeserializer(new RawMessageDeserialiser(appContainerDefn)) // ← resolves in open()
                .setStartingOffsets(OffsetsInitializer.earliest()).setProperties(extra).build();

        WatermarkStrategy<RawMessage> wms = wmProvider != null ? wmProvider.get() : WatermarkStrategy.noWatermarks();
        DataStreamSource<RawMessage> stream = env.fromSource(source, wms, "kafka-" + topic);
        if (totalPartitions > 0) {
            stream.setParallelism(totalPartitions);
        }
        var raw = stream;
        return raw;
    }
}
