package com.hcltech.rmg.kafka;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Properties;

public final class KafkaSources {
    private KafkaSources() {}

    public static DataStreamSource<RawKafkaData> rawKafkaStream(
            StreamExecutionEnvironment env,
            String bootstrapServers,
            String topic,
            String groupId,
            int parallelism,
            OffsetsInitializer startingOffsets,
            Duration partitionDiscoveryInterval,
            WatermarkStrategyProvider<RawKafkaData> wmProvider
    ) {
        Properties extra = new Properties();
        if (partitionDiscoveryInterval != null) {
            extra.put("partition.discovery.interval.ms", String.valueOf(partitionDiscoveryInterval.toMillis()));
        }

        KafkaSource<RawKafkaData> source = KafkaSource.<RawKafkaData>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setDeserializer(new RawKafkaDataDeserializer())
                .setStartingOffsets(startingOffsets)
                .setProperties(extra)
                .build();

        var stream = env.fromSource(source, wmProvider.get(), "kafka-" + topic);
        if (parallelism > 0) stream.setParallelism(parallelism);
        return stream;
    }
}
