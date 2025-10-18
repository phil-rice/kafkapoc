package com.hcltech.rmg.appcontainer.interfaces;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

public class IAppContainerFactoryCachingTest {

    // --- A tiny test factory class with a public no-arg ctor (required by your resolveFactory) ---
    public static class TestFactory implements IAppContainerFactory<String, String, String, String, String> {
        static final AtomicInteger ctorCount = new AtomicInteger(0);
        static final AtomicInteger createCount = new AtomicInteger(0);
        /**
         * Pluggable behavior for tests: envId -> ErrorsOr<AppContainer>
         */
        static Function<String, ErrorsOr<AppContainer<String, String, String, String, String>>> creator =
                id -> ErrorsOr.error("no creator set for id=" + id);

        public TestFactory() {
            ctorCount.incrementAndGet();
        }

        @Override
        public ErrorsOr<AppContainer<String, String, String, String, String>> create(String id) {
            createCount.incrementAndGet();
            return creator.apply(id);
        }

        static void reset() {
            ctorCount.set(0);
            createCount.set(0);
            creator = id -> ErrorsOr.error("no creator set for id=" + id);
        }
    }

    @BeforeEach
    void setup() {
        // Reset global caches between tests
        IAppContainerFactory.FACTORIES.clear();
        IAppContainerFactory.CONTAINERS.clear();
        TestFactory.reset();
    }

    @AfterEach
    void tearDown() {
        // Just to be explicit—no clearAndClose per your preference
        IAppContainerFactory.FACTORIES.clear();
        IAppContainerFactory.CONTAINERS.clear();
    }

    @Test
    void factory_is_instantiated_once() {
        var f1 = IAppContainerFactory.resolveFactory(TestFactory.class);
        var f2 = IAppContainerFactory.resolveFactory(TestFactory.class);

        assertTrue(f1.isValue(), "first resolveFactory should succeed");
        assertTrue(f2.isValue(), "second resolveFactory should succeed");
        assertSame(f1.getValue().orElseThrow(), f2.getValue().orElseThrow(),
                "same cached factory instance expected");
        assertEquals(1, TestFactory.ctorCount.get(), "factory ctor should run once");
    }

    @Test
    void container_cached_per_env_success() {
        // Two distinct mocks for different envs; same env returns same instance
        var devContainer = mock(AppContainer.class);
        var prodContainer = mock(AppContainer.class);

        TestFactory.creator = id -> switch (id) {
            case "dev" -> ErrorsOr.lift(devContainer);
            case "prod" -> ErrorsOr.lift(prodContainer);
            default -> ErrorsOr.error("unknown env: " + id);
        };

        var dev1 = IAppContainerFactory.resolve(AppContainerDefn.of(TestFactory.class, "dev"));
        var dev2 = IAppContainerFactory.resolve(AppContainerDefn.of(TestFactory.class, "dev"));
        var prod1 = IAppContainerFactory.resolve(AppContainerDefn.of(TestFactory.class, "prod"));

        assertTrue(dev1.isValue());
        assertTrue(dev2.isValue());
        assertTrue(prod1.isValue());

        assertSame(devContainer, dev1.getValue().orElseThrow());
        assertSame(devContainer, dev2.getValue().orElseThrow(),
                "same instance from cache for same env");
        assertSame(prodContainer, prod1.getValue().orElseThrow(),
                "different env gets its own instance");

        // 1 factory instantiation + 2 creates (dev, prod) despite 3 resolves
        assertEquals(1, TestFactory.ctorCount.get(), "factory instantiated once");
        assertEquals(2, TestFactory.createCount.get(), "create called once per envId");
    }

    @Test
    void container_error_is_cached() {
        TestFactory.creator = id -> ErrorsOr.error("boom for env " + id);

        var first = IAppContainerFactory.resolve(AppContainerDefn.of(TestFactory.class, "dev"));
        var second = IAppContainerFactory.resolve(AppContainerDefn.of(TestFactory.class, "dev"));

        assertTrue(first.isError());
        assertTrue(second.isError());
        assertEquals(first.getErrors(), second.getErrors(),
                "same cached error should be returned");
        assertEquals(1, TestFactory.createCount.get(),
                "create called only once for failing env");
    }

    @Test
    void different_envs_cache_independently() {
        var c1 = mock(AppContainer.class);
        var c2 = mock(AppContainer.class);
        TestFactory.creator = id -> switch (id) {
            case "e1" -> ErrorsOr.lift(c1);
            case "e2" -> ErrorsOr.lift(c2);
            default -> ErrorsOr.error("unknown " + id);
        };

        var e1a = IAppContainerFactory.resolve(AppContainerDefn.of(TestFactory.class, "e1"));
        var e2a = IAppContainerFactory.resolve(AppContainerDefn.of(TestFactory.class, "e2"));
        var e1b = IAppContainerFactory.resolve(AppContainerDefn.of(TestFactory.class, "e1"));

        assertTrue(e1a.isValue());
        assertTrue(e2a.isValue());
        assertTrue(e1b.isValue());

        assertSame(c1, e1a.getValue().orElseThrow());
        assertSame(c2, e2a.getValue().orElseThrow());
        assertSame(c1, e1b.getValue().orElseThrow());

        assertEquals(2, TestFactory.createCount.get(),
                "create called once for each env");
    }
}
