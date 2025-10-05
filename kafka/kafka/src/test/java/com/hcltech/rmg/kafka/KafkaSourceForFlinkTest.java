package com.hcltech.rmg.kafka;

import com.hcltech.rmg.messages.RawMessage;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

final class KafkaSourceForFlinkWiringTest {

    @Test
    @DisplayName("rawKafkaStream: sets operator name, parallelism, and type")
    void wiring_ok() {
        // env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int parallelism = 12;

        // simple WM provider
        WatermarkStrategyProvider<RawMessage> wmProvider = () -> WatermarkStrategy.noWatermarks();

        // NOTE: new signature: first arg is containerId ("prod" or "test")
        DataStreamSource<RawMessage> stream = KafkaSourceForFlink.rawKafkaStream(
                "test",                        // containerId (resolved in deserializer.open())
                env,
                "dummy:9092",                  // won't connect; we don't execute in this test
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
}
