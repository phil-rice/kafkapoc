// src/test/java/com/hcltech/rmg/appcontainer/impl/AppContainerFactoryTest.java
package com.hcltech.rmg.appcontainer.impl;

import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import org.codehaus.stax2.validation.XMLValidationSchema;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public final class AppContainerFactoryTest {

    @AfterEach
    void tearDown() {
        AppContainerFactory.clearCache();
    }

    // --- Core resolve/identity tests ----------------------------------------

    @Test
    @DisplayName("resolve(prod): returns same singleton for same id")
    void resolve_sameId_sameInstance() {
        AppContainer<KafkaConfig, XMLValidationSchema> a = AppContainerFactory.resolve("prod").valueOrThrow();
        AppContainer<KafkaConfig, XMLValidationSchema> b = AppContainerFactory.resolve("prod").valueOrThrow();
        assertSame(a, b, "Expected same cached instance for the same id");
    }

    @Test
    @DisplayName("resolve: id normalization (trim + lowercase)")
    void resolve_normalizesId() {
        AppContainer<KafkaConfig, XMLValidationSchema> a = AppContainerFactory.resolve(" prod ").valueOrThrow();
        AppContainer<KafkaConfig, XMLValidationSchema> b = AppContainerFactory.resolve("PROD").valueOrThrow();
        assertSame(a, b, "Ids differing only by case/whitespace should map to same instance");
    }

    @Test
    @DisplayName("resolve: different ids yield different instances")
    void resolve_differentIds_differentInstances() {
        AppContainer<KafkaConfig, XMLValidationSchema> prod = AppContainerFactory.resolve("prod").valueOrThrow();
        AppContainer<KafkaConfig, XMLValidationSchema> test = AppContainerFactory.resolve("test").valueOrThrow();
        assertNotSame(prod, test, "Different env ids should not return the same instance");
    }

    // --- prod environment characteristics -----------------------------------

    @Test
    @DisplayName("prod: time service is close to system clock")
    void prod_timeService_closeToNow() {
        AppContainer<KafkaConfig, XMLValidationSchema> prod = AppContainerFactory.resolve("prod").valueOrThrow();

        long before = System.currentTimeMillis();
        long t = prod.time().currentTimeMillis();
        long after = System.currentTimeMillis();

        long toleranceMs = 250;
        assertTrue(
                t >= before - toleranceMs && t <= after + toleranceMs,
                "prod time should be within " + toleranceMs + "ms of System.currentTimeMillis()"
        );
    }

    @Test
    @DisplayName("prod: keyPath is hardcoded")
    void prod_keyPath_isHardcoded() {
        AppContainer<KafkaConfig, XMLValidationSchema> prod = AppContainerFactory.resolve("prod").valueOrThrow();
        List<String> path = prod.keyPath();
        List<String> expected = List.of("domainId");
        assertEquals(expected, path, "prod keyPath should be the hardcoded default");
        assertEquals(expected, AppContainerFactory.resolve("PROD").valueOrThrow().keyPath());
    }

    @Test
    @DisplayName("prod: uuid generator yields different values across calls")
    void prod_uuid_isRandomEnough() {
        AppContainer<KafkaConfig, XMLValidationSchema> prod = AppContainerFactory.resolve("prod").valueOrThrow();
        String u1 = prod.uuid().generate();
        String u2 = prod.uuid().generate();
        assertNotNull(u1);
        assertNotNull(u2);
        assertNotEquals(u1, u2, "Two consecutive UUIDs should differ");
    }

    @Test
    @DisplayName("XML: xml.extractId works on a simple path")
    void prod_xml_services_present_and_work() {
        AppContainer<KafkaConfig, XMLValidationSchema> prod = AppContainerFactory.resolve("prod").valueOrThrow();
        var xml = prod.xml();
        assertNotNull(xml, "xml typeclass should be provided");

        ErrorsOr<String> id = xml.extractId(
                "<Envelope><Body><Order><Id>ABC-123</Id></Order></Body></Envelope>",
                List.of("Envelope", "Body", "Order", "Id")
        );
        assertTrue(id.isValue(), "Expected xml.extractId to succeed on simple XML");
        assertEquals("ABC-123", id.valueOrThrow());
    }

    @Test
    @DisplayName("prod: eventSourceConfig (Kafka) is constructed")
    void prod_eventSourceConfig_present() {
        AppContainer<KafkaConfig, XMLValidationSchema> prod = AppContainerFactory.resolve("prod").valueOrThrow();
        KafkaConfig cfg = prod.eventSourceConfig();
        assertNotNull(cfg, "KafkaConfig should be present");

        assertNotNull(cfg.bootstrapServers(), "bootstrapServers should be set");
        assertNotNull(cfg.topic(), "topic should be set");
        assertFalse(cfg.topic().isBlank(), "topic should not be blank");

        assertNotNull(cfg.startingOffsets(), "startingOffsets should be set");
        assertTrue(
                "earliest".equals(cfg.startingOffsets()) || "latest".equals(cfg.startingOffsets()),
                "startingOffsets should be 'earliest' or 'latest'"
        );

        assertNotNull(cfg.toOffsetsInitializer(), "toOffsetsInitializer should map to a valid OffsetsInitializer");
    }

    @Test
    @DisplayName("prod: rootConfig is loaded, schema map contains schema, and env marker is prod")
    void prod_rootConfig_loaded() {
        AppContainer<KafkaConfig, XMLValidationSchema> prod = AppContainerFactory.resolve("prod").valueOrThrow();
        RootConfig rc = prod.rootConfig();
        assertNotNull(rc, "rootConfig should be loaded for prod");

        // If xmlSchemaPath is set, nameToSchemaMap should contain it
        String schemaName = rc.xmlSchemaPath();
        if (schemaName != null && !schemaName.isBlank()) {
            assertTrue(prod.nameToSchemaMap().containsKey(schemaName),
                    "nameToSchemaMap should contain " + schemaName);
        }

        // differentiate prod vs test by parameter default
        var params = rc.parameterConfig().parameters();
        assertFalse(params.isEmpty());
        assertEquals("prod", params.get(0).defaultValue(), "prod env should default to 'prod'");
    }
    @Test
    @DisplayName("prod: extractors are wired and non-null")
    void prod_extractors_present() {
        AppContainer<KafkaConfig, XMLValidationSchema> prod = AppContainerFactory.resolve("prod").valueOrThrow();

        assertNotNull(prod.eventTypeExtractor(), "eventTypeExtractor should be present");

        // prod extractor expects ["msg","eventType"]
        String evt = prod.eventTypeExtractor().extractEventType(
                Map.of("msg", Map.of("eventType", "Arrived"))
        );
        assertEquals("Arrived", evt);

        assertNotNull(prod.domainTypeExtractor(), "domainTypeExtractor should be present");
        String dom = prod.domainTypeExtractor().extractDomainType(Map.of());
        assertEquals("parcel", dom);
    }


    // --- test environment characteristics -----------------------------------

    @Test
    @DisplayName("test env: deterministic defaults are returned")
    void test_env_defaults() {
        AppContainer<KafkaConfig, XMLValidationSchema> test = AppContainerFactory.resolve("test").valueOrThrow();
        assertEquals(1_726_000_000_000L, test.time().currentTimeMillis());
        assertEquals("11111111-2222-3333-4444-555555555555", test.uuid().generate());
        assertNotNull(test.xml());
    }

    @Test
    @DisplayName("test env: eventSourceConfig (Kafka) is constructed")
    void test_eventSourceConfig_present() {
        AppContainer<KafkaConfig, XMLValidationSchema> test = AppContainerFactory.resolve("test").valueOrThrow();
        KafkaConfig cfg = test.eventSourceConfig();
        assertNotNull(cfg, "KafkaConfig should be present");

        assertNotNull(cfg.bootstrapServers(), "bootstrapServers should be set");
        assertNotNull(cfg.topic(), "topic should be set");
        assertFalse(cfg.topic().isBlank(), "topic should not be blank");

        assertNotNull(cfg.startingOffsets(), "startingOffsets should be set");
        assertTrue(
                "earliest".equals(cfg.startingOffsets()) || "latest".equals(cfg.startingOffsets()),
                "startingOffsets should be 'earliest' or 'latest'"
        );

        assertNotNull(cfg.toOffsetsInitializer(), "toOffsetsInitializer should map to a valid OffsetsInitializer");
    }

    @Test
    @DisplayName("test env: rootConfig is loaded from classpath and env marker is dev")
    void test_rootConfig_loaded() {
        AppContainer<KafkaConfig, XMLValidationSchema> test = AppContainerFactory.resolve("test").valueOrThrow();
        RootConfig rc = test.rootConfig();
        assertNotNull(rc, "rootConfig should be loaded for test");

        var params = rc.parameterConfig().parameters();
        assertFalse(params.isEmpty());
        assertEquals("dev", params.get(0).defaultValue(), "test env should default to 'dev'");
    }

    // --- concurrency / lifecycle --------------------------------------------

    @Test
    @DisplayName("concurrency: resolve is thread-safe and returns the same instance")
    void resolve_threadSafe_singletonPerId() throws Exception {
        int threads = Math.max(4, Runtime.getRuntime().availableProcessors());
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            Set<AppContainer<KafkaConfig, XMLValidationSchema>> results = ConcurrentHashMap.newKeySet();
            CountDownLatch start = new CountDownLatch(1);

            var tasks = IntStream.range(0, 64).mapToObj(i -> pool.submit(() -> {
                start.await();
                return AppContainerFactory.resolve("prod").valueOrThrow();
            })).toList();

            start.countDown();
            for (Future<AppContainer<KafkaConfig, XMLValidationSchema>> f : tasks) {
                results.add(f.get(3, TimeUnit.SECONDS));
            }

            assertEquals(1, results.size(), "All concurrent resolves for same id should return the same instance");
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("clearCache: clears cache; next resolve produces a fresh instance")
    void clearCache_createsFreshInstance() {
        AppContainer<KafkaConfig, XMLValidationSchema> a = AppContainerFactory.resolve("prod").valueOrThrow();
        AppContainerFactory.clearCache();
        AppContainer<KafkaConfig, XMLValidationSchema> b = AppContainerFactory.resolve("prod").valueOrThrow();
        assertNotSame(a, b, "After clearCache, a fresh instance should be created on next resolve");
    }

    // --- argument validation / error paths -----------------------------------

    @Test
    @DisplayName("resolve: null id throws NPE")
    void resolve_nullId_throws() {
        assertThrows(NullPointerException.class, () -> AppContainerFactory.resolve(null));
    }

    @Test
    @DisplayName("resolve: unknown id yields ErrorsOr.error")
    void resolve_unknown_returnsError() {
        ErrorsOr<AppContainer<KafkaConfig, XMLValidationSchema>> eo = AppContainerFactory.resolve("nope");
        assertTrue(eo.isError(), "Unknown id should return ErrorsOr.error");
        var errors = eo.errorsOrThrow();
        assertFalse(errors.isEmpty());
        assertTrue(errors.get(0).toLowerCase().contains("unknown"), "Error should mention unknown id");
    }
}
