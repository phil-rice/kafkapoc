package com.hcltech.rmg.execution.aspects;

import com.hcltech.rmg.common.function.Callback;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

public class AspectExecutorRepositoryTest {

    // ----- Test utilities -----
    /**
     * Bridge the async-shaped executor to a sync call for tests.
     * Unwraps exceptions so assertThrows(...) can match original types.
     */
    private static <C, I, O> O callSync(
            AspectExecutorAsync<C, I, O> exec, String key, C component, I input) {
        CompletableFuture<O> f = new CompletableFuture<>();
        exec.call(key, component, input, new Callback<>() {
            @Override public void success(O value) { f.complete(value); }
            @Override public void failure(Throwable error) { f.completeExceptionally(error); }
        });
        try {
            return f.get(); // allows unwrapping ExecutionException
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) throw re;
            if (cause instanceof Error err) throw err;
            throw new RuntimeException(cause);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        }
    }

    // ----- Base + concrete component classes for class-based routing -----
    static class TestComponent {
        final String tag;
        TestComponent(String tag) { this.tag = tag; }
    }

    static final class XsltComponent extends TestComponent { XsltComponent() { super("xslt"); } }
    static final class JsonComponent extends TestComponent { JsonComponent() { super("json"); } }
    static final class XmlComponent  extends TestComponent { XmlComponent()  { super("xml"); } } // "alias" to XSLT executor
    static final class CsvComponent  extends TestComponent { CsvComponent()  { super("csv"); } } // unregistered
    static final class BadComponent  extends TestComponent { BadComponent()  { super("bad"); } } // throws

    // ----- Executors (sync; wrapped to async by registerSync) -----
    /** returns input + "_xslt" */
    private static final AspectExecutorSync<TestComponent, String, String> XSLT =
            (key, component, input) -> input + "_xslt";

    /** returns input + "_json" */
    private static final AspectExecutorSync<TestComponent, String, String> JSON =
            (key, component, input) -> input + "_json";

    /** throws to exercise defensive behavior */
    private static final AspectExecutorSync<TestComponent, String, String> THROWER =
            (key, component, input) -> { throw new RuntimeException("boom"); };

    // ----- Tests -----

    @Test
    @DisplayName("routes by exact class to the right executor (happy path)")
    void routesByClass() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .registerSync(XsltComponent.class, XSLT)
                .registerSync(JsonComponent.class, JSON);

        var exec = repo.build();

        var r1 = callSync(exec, "k1", new XsltComponent(), "hello");
        assertEquals("hello_xslt", r1);

        var r2 = callSync(exec, "k2", new JsonComponent(), "hello");
        assertEquals("hello_json", r2);
    }

    @Test
    @DisplayName("multiple classes can map to the same executor (alias-like)")
    void multipleClassesToSameExecutor() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .registerSync(XsltComponent.class, XSLT)
                .registerSync(XmlComponent.class,  XSLT); // same behavior for XmlComponent

        var exec = repo.build();

        var r1 = callSync(exec, "k1", new XsltComponent(), "hi");
        assertEquals("hi_xslt", r1);

        var r2 = callSync(exec, "k2", new XmlComponent(), "hi");
        assertEquals("hi_xslt", r2);
    }

    @Test
    @DisplayName("unknown class fails via callback with helpful message")
    void unknownClass() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .registerSync(XsltComponent.class, XSLT);

        var exec = repo.build();

        var ex = assertThrows(IllegalStateException.class,
                () -> callSync(exec, "k", new CsvComponent(), "x"));
        var msg = ex.getMessage().toLowerCase();
        assertTrue(msg.contains("unknown component class"));
        assertTrue(msg.contains(CsvComponent.class.getName().toLowerCase()));
    }

    @Test
    @DisplayName("null component is handled (NPE via callback with message)")
    void nullComponent() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .registerSync(XsltComponent.class, XSLT);

        var exec = repo.build();

        var ex = assertThrows(NullPointerException.class,
                () -> callSync(exec, "k", null, "x"));
        assertTrue(ex.getMessage().toLowerCase().contains("component is null"));
    }

    @Test
    @DisplayName("executor exceptions are surfaced (no boxing)")
    void executorThrows() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .registerSync(BadComponent.class, THROWER);

        var exec = repo.build();

        var ex = assertThrows(RuntimeException.class,
                () -> callSync(exec, "k", new BadComponent(), "x"));
        var msg = ex.getMessage().toLowerCase();
        assertTrue(msg.contains("boom"));
    }

    @Test
    @DisplayName("build snapshot is immutable w.r.t. later registrations")
    void buildSnapshotImmutability() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .registerSync(XsltComponent.class, XSLT);

        var first = repo.build();
        // later registration (should not affect 'first')
        repo.registerSync(JsonComponent.class, JSON);
        var second = repo.build();

        // first snapshot doesn't know JsonComponent -> fails
        assertThrows(IllegalStateException.class,
                () -> callSync(first, "k", new JsonComponent(), "hello"));

        // second snapshot knows it -> works
        var r2 = callSync(second, "k", new JsonComponent(), "hello");
        assertEquals("hello_json", r2);
    }

    @Test
    @DisplayName("duplicate registration fails fast")
    void duplicateRegistration() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .registerSync(XsltComponent.class, XSLT);

        var ex = assertThrows(IllegalStateException.class,
                () -> repo.registerSync(XsltComponent.class, JSON));
        assertTrue(ex.getMessage().toLowerCase().contains("already registered"));
    }

    @Test
    @DisplayName("exact-class routing: superclass registration does not catch subclass")
    void exactClassOnly() {
        class Parent extends TestComponent { Parent() { super("p"); } }
        class Child  extends Parent { Child() { super(); } }

        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .registerSync(Parent.class, XSLT);

        var exec = repo.build();

        // Parent works
        assertEquals("x_xslt", callSync(exec, "k", new Parent(), "x"));

        // Child should fail because routing is exact by runtime class
        var ex = assertThrows(IllegalStateException.class,
                () -> callSync(exec, "k", new Child(), "x"));
        assertTrue(ex.getMessage().toLowerCase().contains("unknown component class"));
    }

    @Test
    @DisplayName("registerSync(null, ...) and registerSync(..., null) throw NPE")
    void registerNulls() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>();
        assertThrows(NullPointerException.class, () -> repo.registerSync(null, XSLT));
        assertThrows(NullPointerException.class, () -> repo.registerSync(XsltComponent.class, null));
    }

    @Test
    @DisplayName("getRegisteredTypes returns an unmodifiable snapshot of keys")
    void registeredTypesUnmodifiable() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .registerSync(XsltComponent.class, XSLT);

        var types = repo.getRegisteredTypes();
        assertTrue(types.contains(XsltComponent.class));
        assertThrows(UnsupportedOperationException.class, () -> types.add(JsonComponent.class));
    }

    @Test
    @DisplayName("concurrent executes across classes (smoke test)")
    void concurrentExecutes() throws Exception {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .registerSync(XsltComponent.class, XSLT)
                .registerSync(JsonComponent.class, JSON);
        var exec = repo.build();

        var pool = java.util.concurrent.Executors.newFixedThreadPool(8);
        try {
            var futures = java.util.stream.IntStream.range(0, 1000)
                    .mapToObj(i -> pool.submit(() -> {
                        if (i % 2 == 0) {
                            return callSync(exec, "k", new XsltComponent(), "a");
                        } else {
                            return callSync(exec, "k", new JsonComponent(), "b");
                        }
                    }))
                    .toList();

            for (var f : futures) {
                var s = f.get();
                assertTrue("a_xslt".equals(s) || "b_json".equals(s));
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
