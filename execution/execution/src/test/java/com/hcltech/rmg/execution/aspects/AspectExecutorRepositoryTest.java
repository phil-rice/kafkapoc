package com.hcltech.rmg.execution.aspects;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class AspectExecutorRepositoryTest {

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

    // ----- Executors (base-typed; can be reused by many subclasses) -----
    /** returns input + "_xslt" */
    private static final AspectExecutor<TestComponent, String, String> XSLT =
            (key, component, input) -> input + "_xslt";

    /** returns input + "_json" */
    private static final AspectExecutor<TestComponent, String, String> JSON =
            (key, component, input) -> input + "_json";

    /** throws to exercise defensive behavior */
    private static final AspectExecutor<TestComponent, String, String> THROWER =
            (key, component, input) -> { throw new RuntimeException("boom"); };

    // ----- Tests -----

    @Test
    @DisplayName("routes by exact class to the right executor (happy path)")
    void routesByClass() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .register(XsltComponent.class, XSLT)
                .register(JsonComponent.class, JSON);

        var exec = repo.build();

        var r1 = exec.execute("k1", new XsltComponent(), "hello");
        assertEquals("hello_xslt", r1);

        var r2 = exec.execute("k2", new JsonComponent(), "hello");
        assertEquals("hello_json", r2);
    }

    @Test
    @DisplayName("multiple classes can map to the same executor (alias-like)")
    void multipleClassesToSameExecutor() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .register(XsltComponent.class, XSLT)
                .register(XmlComponent.class,  XSLT); // same behavior for XmlComponent

        var exec = repo.build();

        var r1 = exec.execute("k1", new XsltComponent(), "hi");
        assertEquals("hi_xslt", r1);

        var r2 = exec.execute("k2", new XmlComponent(), "hi");
        assertEquals("hi_xslt", r2);
    }

    @Test
    @DisplayName("unknown class throws with helpful message")
    void unknownClass() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .register(XsltComponent.class, XSLT);

        var exec = repo.build();

        var ex = assertThrows(IllegalStateException.class,
                () -> exec.execute("k", new CsvComponent(), "x"));
        var msg = ex.getMessage().toLowerCase();
        assertTrue(msg.contains("unknown component class"));
        assertTrue(msg.contains(CsvComponent.class.getName().toLowerCase()));
    }

    @Test
    @DisplayName("null component is handled (throws NPE with message)")
    void nullComponent() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .register(XsltComponent.class, XSLT);

        var exec = repo.build();

        var ex = assertThrows(NullPointerException.class,
                () -> exec.execute("k", null, "x"));
        assertTrue(ex.getMessage().toLowerCase().contains("component is null"));
    }

    @Test
    @DisplayName("executor exceptions are surfaced (no boxing)")
    void executorThrows() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .register(BadComponent.class, THROWER);

        var exec = repo.build();

        var ex = assertThrows(RuntimeException.class,
                () -> exec.execute("k", new BadComponent(), "x"));
        var msg = ex.getMessage().toLowerCase();
        assertTrue(msg.contains("boom"));
    }

    @Test
    @DisplayName("build snapshot is immutable w.r.t. later registrations")
    void buildSnapshotImmutability() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .register(XsltComponent.class, XSLT);

        var first = repo.build();
        // later registration (should not affect 'first')
        repo.register(JsonComponent.class, JSON);
        var second = repo.build();

        // first snapshot doesn't know JsonComponent -> throws
        assertThrows(IllegalStateException.class,
                () -> first.execute("k", new JsonComponent(), "hello"));

        // second snapshot knows it -> works
        var r2 = second.execute("k", new JsonComponent(), "hello");
        assertEquals("hello_json", r2);
    }

    @Test
    @DisplayName("duplicate registration fails fast")
    void duplicateRegistration() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .register(XsltComponent.class, XSLT);

        var ex = assertThrows(IllegalStateException.class,
                () -> repo.register(XsltComponent.class, JSON));
        assertTrue(ex.getMessage().toLowerCase().contains("already registered"));
    }
    @Test
    @DisplayName("exact-class routing: superclass registration does not catch subclass")
    void exactClassOnly() {
        class Parent extends TestComponent { Parent() { super("p"); } }
        class Child  extends Parent { Child() { super(); } }

        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .register(Parent.class, XSLT);

        var exec = repo.build();

        // Parent works
        assertEquals("x_xslt", exec.execute("k", new Parent(), "x"));

        // Child should fail because routing is exact by runtime class
        var ex = assertThrows(IllegalStateException.class,
                () -> exec.execute("k", new Child(), "x"));
        assertTrue(ex.getMessage().toLowerCase().contains("unknown component class"));
    }
    @Test
    @DisplayName("register(null, ...) and register(..., null) throw NPE")
    void registerNulls() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>();
        assertThrows(NullPointerException.class, () -> repo.register(null, XSLT));
        assertThrows(NullPointerException.class, () -> repo.register(XsltComponent.class, null));
    }
    @Test
    @DisplayName("getRegisteredTypes returns an unmodifiable snapshot of keys")
    void registeredTypesUnmodifiable() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .register(XsltComponent.class, XSLT);

        var types = repo.getRegisteredTypes();
        assertTrue(types.contains(XsltComponent.class));
        assertThrows(UnsupportedOperationException.class, () -> types.add(JsonComponent.class));
    }
    @Test
    @DisplayName("concurrent executes across classes (smoke test)")
    void concurrentExecutes() throws Exception {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .register(XsltComponent.class, XSLT)
                .register(JsonComponent.class, JSON);
        var exec = repo.build();

        var pool = java.util.concurrent.Executors.newFixedThreadPool(8);
        try {
            var futures = java.util.stream.IntStream.range(0, 1000)
                    .mapToObj(i -> pool.submit(() ->
                            i % 2 == 0
                                    ? exec.execute("k", new XsltComponent(), "a")
                                    : exec.execute("k", new JsonComponent(), "b")))
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
