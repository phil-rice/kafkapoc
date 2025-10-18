package com.hcltech.rmg.shared_worker;

import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.kafka.KafkaSourceForFlink;
import com.hcltech.rmg.kafka.WatermarkStrategyProvider;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import com.hcltech.rmg.messages.RawMessage;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class KafkaFlinkHelper {
    static public <ESC, CepState, Msg, Schema, MetricsParam> DataStreamSource<RawMessage> createRawMessageStreamFromKafka(AppContainerDefn<ESC, CepState, Msg, Schema, MetricsParam> appContainerDefn, StreamExecutionEnvironment env, KafkaConfig kafka, int checkpointingInterval) {
        final int totalPartitions = kafka.sourceParallelism();
        env.setParallelism(totalPartitions > 0 ? totalPartitions : env.getParallelism());
        env.getConfig().setAutoWatermarkInterval(0);
        env.enableCheckpointing(checkpointingInterval);

        var raw = KafkaSourceForFlink.rawKafkaStream(appContainerDefn, env, kafka.bootstrapServers(), kafka.topic(), kafka.groupId(), totalPartitions, OffsetsInitializer.earliest(), Duration.ofSeconds(60), WatermarkStrategyProvider.none());
        return raw;
    }
}
