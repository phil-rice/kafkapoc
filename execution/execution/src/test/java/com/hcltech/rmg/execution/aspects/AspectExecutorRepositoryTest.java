package com.hcltech.rmg.execution.aspects;

import com.hcltech.rmg.common.HasType;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AspectExecutorRepositoryTest {

    // ----- Simple component -----
    static final class TestComponent implements HasType {
        private final String type;

        TestComponent(String type) {
            this.type = type;
        }

        @Override
        public String type() {
            return type;
        }
    }

    // ----- Registered executors used in tests -----
    /**
     * returns input + "_xslt"
     */
    private static final RegisteredAspectExecutor<TestComponent, String, String> XSLT =
            (key, modules, aspect, component, input) -> ErrorsOr.lift(input + "_xslt");

    /**
     * returns input + "_json"
     */
    private static final RegisteredAspectExecutor<TestComponent, String, String> JSON =
            (key, modules, aspect, component, input) -> ErrorsOr.lift(input + "_json");

    /**
     * throws to exercise defensive catch -> repository should convert to ErrorsOr.error(...)
     */
    private static final RegisteredAspectExecutor<TestComponent, String, String> THROWER =
            (key, modules, aspect, component, input) -> {
                throw new RuntimeException("boom");
            };

    // ----- Tests -----

    @Test
    @DisplayName("routes by exact type to the right executor (happy path)")
    void routesByType() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .register("xslt", XSLT)
                .register("json", JSON);

        var executor = repo.build(List.of("mA", "mB"), "transform");

        var r1 = executor.execute("key", new TestComponent("xslt"), "hello");
        assertTrue(r1.isValue(), "expected value for xslt");
        assertEquals("hello_xslt", r1.valueOrThrow());

        var r2 = executor.execute("key", new TestComponent("json"), "hello");
        assertTrue(r2.isValue(), "expected value for json");
        assertEquals("hello_json", r2.valueOrThrow());
    }

    @Test
    @DisplayName("alias points to the same executor")
    void aliasWorks() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .register("xslt", XSLT)
                .alias("xslt", "xml");

        var executor = repo.build(List.of("m1"), "transform");

        var r = executor.execute("key", new TestComponent("xml"), "hi");
        assertTrue(r.isValue(), "expected value for alias");
        assertEquals("hi_xslt", r.valueOrThrow());
    }

    @Test
    @DisplayName("unknown type yields ErrorsOr.error with helpful message")
    void unknownType() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .register("xslt", XSLT);

        var executor = repo.build(List.of("m1"), "transform");

        var r = executor.execute("key", new TestComponent("csv"), "x");
        assertTrue(r.isError(), "expected error for unknown type");
        var msg = String.join(" | ", r.errorsOrThrow()).toLowerCase();
        assertTrue(msg.contains("unknown component type"), "error message should mention unknown type");
        assertTrue(msg.contains("csv"), "error message should include the unknown type string");
    }

    @Test
    @DisplayName("null component is handled")
    void nullComponent() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .register("xslt", XSLT);

        var executor = repo.build(List.of("m1"), "transform");

        var r = executor.execute("key", null, "x");
        assertTrue(r.isError());
        assertTrue(String.join(" | ", r.errorsOrThrow()).toLowerCase().contains("component is null"));
    }

    @Test
    @DisplayName("empty type is handled")
    void emptyType() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .register("xslt", XSLT);

        var executor = repo.build(List.of("m1"), "transform");

        var r = executor.execute("key", new TestComponent(""), "x");
        assertTrue(r.isError());
        assertTrue(String.join(" | ", r.errorsOrThrow()).toLowerCase().contains("type is null/empty"));
    }

    @Test
    @DisplayName("executor exceptions are caught and surfaced as ErrorsOr.error")
    void executorThrows() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .register("bad", THROWER);

        var executor = repo.build(List.of("m1"), "transform");

        var r = executor.execute("key", new TestComponent("bad"), "x");
        assertTrue(r.isError(), "expected error when executor throws");
        var msg = String.join(" | ", r.errorsOrThrow()).toLowerCase();
        assertTrue(msg.contains("threw"), "error should mention that the executor threw");
        assertTrue(msg.contains("boom"), "original exception message should be included");
    }

    @Test
    @DisplayName("build snapshot is immutable w.r.t. later registrations")
    void buildSnapshotImmutability() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .register("xslt", XSLT);

        var first = repo.build(List.of("m1"), "transform");
        // later registration
        repo.register("json", JSON);
        var second = repo.build(List.of("m1"), "transform");

        var r1 = first.execute("key", new TestComponent("json"), "hello");
        assertTrue(r1.isError(), "first build must not know 'json'");

        var r2 = second.execute("key", new TestComponent("json"), "hello");
        assertTrue(r2.isValue());
        assertEquals("hello_json", r2.valueOrThrow());
    }

    @Test
    @DisplayName("duplicate registration fails fast")
    void duplicateRegistration() {
        var repo = new AspectExecutorRepository<TestComponent, String, String>()
                .register("xslt", XSLT);

        var ex = assertThrows(IllegalStateException.class, () -> repo.register("xslt", JSON));
        assertTrue(ex.getMessage().toLowerCase().contains("already registered"));
    }
}
