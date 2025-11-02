package com.hcltech.rmg.worker.eventhubtest;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

public class EventHubCopyJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Replace these with your actual Event Hub Kafka-compatible connection details
        String bootstrapServers = "your-eventhub-namespace.servicebus.windows.net:9093";
        String inputTopic = "source-topic";
        String outputTopic = "sink-topic";
        String consumerGroup = "$Default";
        String saslUsername = "$ConnectionString";
        String saslPassword = "Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...";

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId(consumerGroup)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("security.protocol", "SASL_SSL")
                .setProperty("sasl.mechanism", "PLAIN")
                .setProperty("sasl.jaas.config",
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" +
                                saslUsername + "\" password=\"" + saslPassword + "\";")
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(outputTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setKafkaProducerConfig(java.util.Map.of(
                        "security.protocol", "SASL_SSL",
                        "sasl.mechanism", "PLAIN",
                        "sasl.jaas.config",
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" +
                                saslUsername + "\" password=\"" + saslPassword + "\";"
                ))
                .build();

        env.fromSource(source, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "EventHubSource")
           .sinkTo(sink)
           .name("EventHubSink");

        env.execute("EventHub Copy Job");
    }
}
