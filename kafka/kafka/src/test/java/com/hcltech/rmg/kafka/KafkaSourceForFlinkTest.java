package com.hcltech.rmg.kafka;

import com.hcltech.rmg.appcontainer.impl.AppContainerFactoryForMapStringObject;
import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.messages.RawMessage;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

final class KafkaSourceForFlinkWiringTest {

    @Test
    @DisplayName("rawKafkaStream: sets operator name, parallelism, and type")
    void wiring_ok() {
        // env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int parallelism = 12;

        // simple WM provider
        WatermarkStrategyProvider<RawMessage> wmProvider = WatermarkStrategy::noWatermarks;

        // NEW: build an AppContainerDefn with your factory and env id
        var defn = AppContainerDefn.of(AppContainerFactoryForMapStringObject.class, "test");

        DataStreamSource<RawMessage> stream = KafkaSourceForFlink.rawKafkaStream(
                defn,                      // container defn (resolved in deserializer.open())
                env,
                "dummy:9092",              // won't connect; we don't execute in this test
                "t",
                "g",
                parallelism,
                OffsetsInitializer.earliest(),
                Duration.ofSeconds(60),
                wmProvider
        );

        assertEquals("kafka-t", stream.getTransformation().getName());
        assertEquals(parallelism, stream.getTransformation().getParallelism());
        assertEquals(TypeInformation.of(RawMessage.class), stream.getType());
    }

    @Test
    @DisplayName("rawKafkaStream: merges provided extra Kafka properties")
    void rawKafkaStream_mergesExtraProperties() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        WatermarkStrategyProvider<RawMessage> wmProvider = WatermarkStrategy::noWatermarks;
        var defn = AppContainerDefn.of(AppContainerFactoryForMapStringObject.class, "test");

        Properties extra = new Properties();
        extra.put("security.protocol", "SASL_SSL");
        extra.put("sasl.mechanism", "PLAIN");

        DataStreamSource<RawMessage> stream = KafkaSourceForFlink.rawKafkaStream(
                defn,
                env,
                "dummy:9092",
                "t",
                "g",
                1,
                OffsetsInitializer.earliest(),
                Duration.ofSeconds(30),
                wmProvider,
                extra
        );

        Object transformation = stream.getTransformation();
        Object source = transformation.getClass().getMethod("getSource").invoke(transformation);
        assertEquals("org.apache.flink.connector.kafka.source.KafkaSource", source.getClass().getName());

        Field propsField = source.getClass().getDeclaredField("props");
        propsField.setAccessible(true);
        Properties properties = (Properties) propsField.get(source);

        assertEquals("SASL_SSL", properties.getProperty("security.protocol"));
        assertEquals("PLAIN", properties.getProperty("sasl.mechanism"));
        assertEquals(String.valueOf(Duration.ofSeconds(30).toMillis()), properties.getProperty("partition.discovery.interval.ms"));
    }
}
