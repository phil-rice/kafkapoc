package com.hcltech.rmg.execution.aspects;

import com.hcltech.rmg.common.function.Callback;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class AspectExecutorRepositoryTest {

    // ----- helpers -----
    static final class CapturingCallback<T> implements Callback<T> {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<T> value = new AtomicReference<>();
        final AtomicReference<Throwable> error = new AtomicReference<>();

        @Override public void success(T v) { value.set(v); latch.countDown(); }
        @Override public void failure(Throwable t) { error.set(t); latch.countDown(); }

        T awaitSuccess() throws InterruptedException {
            assertTrue(latch.await(2, TimeUnit.SECONDS), "callback timed out");
            if (error.get() != null) {
                fail("Expected success but got failure: " + error.get());
            }
            return value.get();
        }

        Throwable awaitFailure() throws InterruptedException {
            assertTrue(latch.await(2, TimeUnit.SECONDS), "callback timed out");
            if (error.get() == null) {
                fail("Expected failure but got success: " + value.get());
            }
            return error.get();
        }
    }

    // ----- Base + concrete component classes for class-based routing -----
    static class TestComponent {
        final String tag;
        TestComponent(String tag) { this.tag = tag; }
    }

    static final class XsltComponent extends TestComponent { XsltComponent() { super("xslt"); } }
    static final class JsonComponent extends TestComponent { JsonComponent() { super("json"); } }
    static final class XmlComponent  extends TestComponent { XmlComponent()  { super("xml"); } } // alias to XSLT executor
    static final class CsvComponent  extends TestComponent { CsvComponent()  { super("csv"); } } // unregistered
    static final class BadComponent  extends TestComponent { BadComponent()  { super("bad"); } } // throws

    // ----- Sync executors (wrapped via registerSync) -----
    /** returns input + "_xslt" */
    private static final AspectExecutor<TestComponent, String, String> XSLT =
            (key, component, input) -> input + "_xslt";

    /** returns input + "_json" */
    private static final AspectExecutor<TestComponent, String, String> JSON =
            (key, component, input) -> input + "_json";

    /** throws to exercise defensive behavior */
    private static final AspectExecutor<TestComponent, String, String> THROWER =
            (key, component, input) -> { throw new RuntimeException("boom"); };

    @Test
    @DisplayName("routes by exact class to the right executor (happy path)")
    void routesByClass() throws Exception {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .registerSync(XsltComponent.class, XSLT)
                .registerSync(JsonComponent.class, JSON);

        var exec = repo.build();

        var cb1 = new CapturingCallback<String>();
        exec.call("k1", new XsltComponent(), "hello", cb1);
        assertEquals("hello_xslt", cb1.awaitSuccess());

        var cb2 = new CapturingCallback<String>();
        exec.call("k2", new JsonComponent(), "hello", cb2);
        assertEquals("hello_json", cb2.awaitSuccess());
    }

    @Test
    @DisplayName("multiple classes can map to the same executor (alias-like)")
    void multipleClassesToSameExecutor() throws Exception {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .registerSync(XsltComponent.class, XSLT)
                .registerSync(XmlComponent.class,  XSLT); // same behavior for XmlComponent

        var exec = repo.build();

        var cb1 = new CapturingCallback<String>();
        exec.call("k1", new XsltComponent(), "hi", cb1);
        assertEquals("hi_xslt", cb1.awaitSuccess());

        var cb2 = new CapturingCallback<String>();
        exec.call("k2", new XmlComponent(), "hi", cb2);
        assertEquals("hi_xslt", cb2.awaitSuccess());
    }

    @Test
    @DisplayName("unknown class fails via callback with helpful message")
    void unknownClass() throws Exception {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .registerSync(XsltComponent.class, XSLT);

        var exec = repo.build();

        var cb = new CapturingCallback<String>();
        exec.call("k", new CsvComponent(), "x", cb);
        var err = cb.awaitFailure();
        assertTrue(err instanceof IllegalStateException);
        var msg = err.getMessage().toLowerCase();
        assertTrue(msg.contains("unknown component class"));
        assertTrue(msg.contains(CsvComponent.class.getName().toLowerCase()));
    }

    @Test
    @DisplayName("null component is handled (failure via callback with NPE)")
    void nullComponent() throws Exception {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .registerSync(XsltComponent.class, XSLT);

        var exec = repo.build();

        var cb = new CapturingCallback<String>();
        exec.call("k", null, "x", cb);
        var err = cb.awaitFailure();
        assertTrue(err instanceof NullPointerException);
        assertTrue(err.getMessage().toLowerCase().contains("component is null"));
    }

    @Test
    @DisplayName("executor exceptions are surfaced via failure callback (no boxing)")
    void executorThrows() throws Exception {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .registerSync(BadComponent.class, THROWER);

        var exec = repo.build();

        var cb = new CapturingCallback<String>();
        exec.call("k", new BadComponent(), "x", cb);
        var err = cb.awaitFailure();
        assertTrue(err instanceof RuntimeException);
        assertTrue(err.getMessage().toLowerCase().contains("boom"));
    }

    @Test
    @DisplayName("build snapshot is immutable w.r.t. later registrations")
    void buildSnapshotImmutability() throws Exception {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .registerSync(XsltComponent.class, XSLT);

        var first = repo.build();
        // later registration (should not affect 'first')
        repo.registerSync(JsonComponent.class, JSON);
        var second = repo.build();

        var cb1 = new CapturingCallback<String>();
        first.call("k", new JsonComponent(), "hello", cb1);
        assertTrue(cb1.awaitFailure() instanceof IllegalStateException); // first snapshot doesn't know JsonComponent

        var cb2 = new CapturingCallback<String>();
        second.call("k", new JsonComponent(), "hello", cb2);
        assertEquals("hello_json", cb2.awaitSuccess()); // second snapshot does
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
    void exactClassOnly() throws Exception {
        class Parent extends TestComponent { Parent() { super("p"); } }
        class Child  extends Parent { Child() { super(); } }

        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .registerSync(Parent.class, XSLT);

        var exec = repo.build();

        var ok = new CapturingCallback<String>();
        exec.call("k", new Parent(), "x", ok);
        assertEquals("x_xslt", ok.awaitSuccess());

        var bad = new CapturingCallback<String>();
        exec.call("k", new Child(), "x", bad);
        var err = bad.awaitFailure();
        assertTrue(err instanceof IllegalStateException);
        assertTrue(err.getMessage().toLowerCase().contains("unknown component class"));
    }

    @Test
    @DisplayName("register(null, ...) and register(..., null) throw NPE")
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
    @DisplayName("concurrent calls across classes (smoke test)")
    void concurrentCalls() throws Exception {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .registerSync(XsltComponent.class, XSLT)
                .registerSync(JsonComponent.class, JSON);
        var exec = repo.build();

        var pool = java.util.concurrent.Executors.newFixedThreadPool(8);
        try {
            var tasks = java.util.stream.IntStream.range(0, 1000).mapToObj(i -> (Runnable) () -> {
                var cb = new CapturingCallback<String>();
                if (i % 2 == 0) exec.call("k", new XsltComponent(), "a", cb);
                else             exec.call("k", new JsonComponent(), "b", cb);
                try {
                    var s = cb.awaitSuccess();
                    assertTrue("a_xslt".equals(s) || "b_json".equals(s));
                } catch (InterruptedException e) {
                    fail(e);
                }
            }).toList();

            for (var r : tasks) pool.submit(r).get();
        } finally {
            pool.shutdownNow();
        }
    }
}
