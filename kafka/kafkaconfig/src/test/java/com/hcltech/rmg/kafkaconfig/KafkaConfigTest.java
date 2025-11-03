package com.hcltech.rmg.kafkaconfig;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class KafkaConfigTest {

    private String oldKafkaPartitions;

    @BeforeEach
    void saveSysProps() {
        oldKafkaPartitions = System.getProperty("kafka.partitions");
    }

    @AfterEach
    void restoreSysProps() {
        if (oldKafkaPartitions == null) {
            System.clearProperty("kafka.partitions");
        } else {
            System.setProperty("kafka.partitions", oldKafkaPartitions);
        }
        // Clean up system properties used in tests
        System.clearProperty("kafka.bootstrap");
        System.clearProperty("kafka.topic");
        System.clearProperty("kafka.group.id");
        System.clearProperty("kafka.source.parallelism");
        System.clearProperty("kafka.starting.offsets");
        System.clearProperty("kafka.partition.discovery.seconds");
    }

    /** Helper: check for "latest" OffsetsInitializer */
    private static void assertIsLatest(OffsetsInitializer oi) {
        assertEquals(OffsetsInitializer.latest().getClass(), oi.getClass(),
                "OffsetsInitializer should be 'latest'");
    }

    /** Helper: check for "earliest" OffsetsInitializer */
    private static void assertIsEarliest(OffsetsInitializer oi) {
        assertEquals(OffsetsInitializer.earliest().getClass(), oi.getClass(),
                "OffsetsInitializer should be 'earliest'");
    }

    @Test
    void defaultsWithEmptyProperties() {
        System.clearProperty("kafka.partitions"); // ensure fallback is 12

        Properties p = new Properties();
        KafkaConfig cfg = KafkaConfig.fromProperties(p, null,false);

        assertEquals("localhost:9092", cfg.bootstrapServer());
        assertEquals("test-topic", cfg.topic());

        assertNotNull(cfg.groupId());
        assertTrue(cfg.groupId().startsWith("g-"), "groupId should default to 'g-{timestamp}'");

        assertEquals(14, cfg.sourceParallelism(), "parallelism should fall back to system default 12");

        // Offsets default to earliest (string + mapping)
        assertEquals("earliest", cfg.startingOffsets(), "startingOffsets should default to 'earliest'");
        assertIsEarliest(cfg.toOffsetsInitializer());

        // Discovery default: 60 seconds
        assertEquals(Duration.ofSeconds(60), cfg.partitionDiscovery());
        assertEquals("60000",
                cfg.properties().getProperty("partition.discovery.interval.ms"),
                "properties should contain discovery interval in ms");
    }

    @Test
    void overridesAllProperties() {
        Properties p = new Properties();
        p.setProperty("kafka.bootstrap", "broker1:9092");
        p.setProperty("kafka.topic", "orders");
        p.setProperty("kafka.group.id", "my-group");
        p.setProperty("kafka.source.parallelism", "5");
        p.setProperty("kafka.starting.offsets", "latest");
        p.setProperty("kafka.partition.discovery.seconds", "30");

        KafkaConfig cfg = KafkaConfig.fromProperties(p, null,false);

        assertEquals("broker1:9092", cfg.bootstrapServer());
        assertEquals("orders", cfg.topic());
        assertEquals("my-group", cfg.groupId());
        assertEquals(5, cfg.sourceParallelism());

        // Offsets as string and initializer mapping
        assertEquals("latest", cfg.startingOffsets());
        assertIsLatest(cfg.toOffsetsInitializer());

        assertEquals(Duration.ofSeconds(30), cfg.partitionDiscovery());
        assertEquals("30000", cfg.properties().getProperty("partition.discovery.interval.ms"));
    }

    @Test
    void usesTopicIfNotNull(){
        Properties p = new Properties();
        p.setProperty("kafka.topic", "orders");

        KafkaConfig cfg = KafkaConfig.fromProperties(p, "overridden-topic",false);

        assertEquals("overridden-topic", cfg.topic(),
                "topic parameter to fromProperties() should override property value");
    }

    @Test
    void discoveryDisabledWhenZeroSeconds() {
        Properties p = new Properties();
        p.setProperty("kafka.partition.discovery.seconds", "0");

        KafkaConfig cfg = KafkaConfig.fromProperties(p, null,false);

        assertNull(cfg.partitionDiscovery(), "discovery should be null when configured as 0");
        assertNull(cfg.properties().getProperty("partition.discovery.interval.ms"),
                "properties should NOT include discovery interval when disabled");
    }

    @Test
    void sourceParallelismPrefersExplicitOverSystemFallback() {
        System.setProperty("kafka.partitions", "99"); // would be fallback if explicit not set

        Properties p = new Properties();
        p.setProperty("kafka.source.parallelism", "7");

        KafkaConfig cfg = KafkaConfig.fromProperties(p,null,false    );
        assertEquals(7, cfg.sourceParallelism(), "explicit kafka.source.parallelism should win over system fallback");
    }

    @Test
    void sourceParallelismFallsBackToSystemKafkaPartitions() {
        System.setProperty("kafka.partitions", "16");

        Properties p = new Properties(); // no kafka.source.parallelism provided
        KafkaConfig cfg = KafkaConfig.fromProperties(p, null,false);

        assertEquals(16, cfg.sourceParallelism(),
                "should use Integer.getInteger(\"kafka.partitions\", 12) fallback when explicit not provided");
    }

    @Test
    void invalidStartingOffsetsFallsBackToEarliest() {
        Properties p = new Properties();
        p.setProperty("kafka.starting.offsets", "INVALID_VALUE");

        KafkaConfig cfg = KafkaConfig.fromProperties(p, null,false);

        assertEquals("earliest", cfg.startingOffsets(), "invalid value should fall back to 'earliest'");
        assertIsEarliest(cfg.toOffsetsInitializer());
    }

    @Test
    void fromSystemPropsReadsSystemProperties() {
        System.setProperty("kafka.bootstrap", "sys:9092");
        System.setProperty("kafka.topic", "sys-topic");
        System.setProperty("kafka.group.id", "sys-group");
        System.setProperty("kafka.source.parallelism", "3");
        System.setProperty("kafka.starting.offsets", "latest");
        System.setProperty("kafka.partition.discovery.seconds", "5");

        KafkaConfig cfg = KafkaConfig.fromSystemProps(false);

        assertEquals("sys:9092", cfg.bootstrapServer());
        assertEquals("sys-topic", cfg.topic());
        assertEquals("sys-group", cfg.groupId());
        assertEquals(3, cfg.sourceParallelism());

        assertEquals("latest", cfg.startingOffsets());
        assertIsLatest(cfg.toOffsetsInitializer());

        assertEquals(Duration.ofSeconds(5), cfg.partitionDiscovery());
        assertEquals("5000", cfg.properties().getProperty("partition.discovery.interval.ms"));
    }
}
