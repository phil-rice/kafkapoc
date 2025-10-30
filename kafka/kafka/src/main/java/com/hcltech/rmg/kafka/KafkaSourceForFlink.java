package com.hcltech.rmg.kafka;

import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.messages.RawMessage;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Properties;

/**
 * Thin wrapper to build a Flink Kafka source for RawMessage.
 * Only a {@code containerId} is serialized; the deserializer resolves dependencies in open().
 */
public final class KafkaSourceForFlink<ESC, CepState, Msg, Schema, RT,MetricsParam> {
    private KafkaSourceForFlink() {
    }

    /**
     * Build a Kafka source stream producing {@link RawMessage}.
     *
     * @param appContainerDefn           App container definition
     * @param env                        Flink StreamExecutionEnvironment
     * @param bootstrapServers           Kafka bootstrap servers
     * @param topic                      Kafka topic
     * @param groupId                    Kafka group id
     * @param parallelism                source operator parallelism (>0 to set, else use env default)
     * @param startingOffsets            starting offsets (e.g., OffsetsInitializer.earliest())
     * @param partitionDiscoveryInterval null to disable; else how often to discover new partitions
     * @param wmProvider                 watermark strategy provider (use WatermarkStrategy.noWatermarks() if none)
     */
    public static <ESC, CepState, Msg, Schema, RT,FR,MetricsParam> DataStreamSource<RawMessage> rawKafkaStream(AppContainerDefn<ESC, CepState, Msg, Schema, RT,FR,MetricsParam> appContainerDefn, StreamExecutionEnvironment env, String bootstrapServers, String topic, String groupId, int parallelism, OffsetsInitializer startingOffsets, Duration partitionDiscoveryInterval, WatermarkStrategyProvider<RawMessage> wmProvider, Properties extraProperties) {
        Properties extra = new Properties();
        if (extraProperties != null) {
            extra.putAll(extraProperties);
        }
        if (partitionDiscoveryInterval != null) {
            extra.putIfAbsent("partition.discovery.interval.ms", String.valueOf(partitionDiscoveryInterval.toMillis()));
        }

        org.apache.flink.connector.kafka.source.KafkaSource<RawMessage> source = org.apache.flink.connector.kafka.source.KafkaSource.<RawMessage>builder().setBootstrapServers(bootstrapServers).setTopics(topic).setGroupId(groupId).setDeserializer(new RawMessageDeserialiser(appContainerDefn)) // ← resolves in open()
                .setStartingOffsets(startingOffsets).setProperties(extra).build();

        WatermarkStrategy<RawMessage> wms = wmProvider != null ? wmProvider.get() : WatermarkStrategy.noWatermarks();
        DataStreamSource<RawMessage> stream = env.fromSource(source, wms, "kafka-" + topic);
        if (parallelism > 0) {
            stream.setParallelism(parallelism);
        }
        return stream;
    }

    /**
     * Convenience overload with no partition discovery and no watermarks.
     */
    public static <ESC, CepState, Msg, Schema,RT,FR, MetricsParam> DataStreamSource<RawMessage> rawKafkaStream(AppContainerDefn<ESC, CepState, Msg, Schema, RT,FR,MetricsParam> appContainerDefn, StreamExecutionEnvironment env, String bootstrapServers, String topic, String groupId, int parallelism, OffsetsInitializer startingOffsets) {
        return rawKafkaStream(appContainerDefn, env, bootstrapServers, topic, groupId, parallelism, startingOffsets, null, WatermarkStrategy::noWatermarks, null);
    }

    public static <ESC, CepState, Msg, Schema, RT,FR,MetricsParam> DataStreamSource<RawMessage> rawKafkaStream(AppContainerDefn<ESC, CepState, Msg, Schema, RT,FR,MetricsParam> appContainerDefn, StreamExecutionEnvironment env, String bootstrapServers, String topic, String groupId, int parallelism, OffsetsInitializer startingOffsets, Duration partitionDiscoveryInterval, WatermarkStrategyProvider<RawMessage> wmProvider) {
        return rawKafkaStream(appContainerDefn, env, bootstrapServers, topic, groupId, parallelism, startingOffsets, partitionDiscoveryInterval, wmProvider, null);
    }
}
