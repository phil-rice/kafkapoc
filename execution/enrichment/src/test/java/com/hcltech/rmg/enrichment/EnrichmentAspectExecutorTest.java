package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.MapStringObjectCepStateTypeClass;
import com.hcltech.rmg.common.function.Callback;
import com.hcltech.rmg.config.enrich.EnrichmentWithDependencies;
import com.hcltech.rmg.execution.aspects.AspectExecutorAsync;
import com.hcltech.rmg.execution.aspects.EnrichmentBatchException;
import com.hcltech.rmg.messages.EnvelopeHeader;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for EnrichmentAspectExecutor with real ValueEnvelope
 * and Map<String,Object> CEP state via MapStringObjectCepStateTypeClass.
 */
@DisplayName("EnrichmentAspectExecutor (Map<String,Object> state)")
class EnrichmentAspectExecutorTest {

    /** Minimal enricher that writes a single key to the CEP state. */
    static final class FakeEnricher implements EnrichmentWithDependencies {
        private final String key;
        FakeEnricher(String key) { this.key = key; }
        @Override public List<List<String>> inputs() { return List.of(); }
        @Override public List<String> output() { return List.of(key); }
        @Override public String toString() { return "Enricher(" + key + ")"; }
    }

    /** Fresh ValueEnvelope with empty state and no prior modifications. */
    private static ValueEnvelope<Map<String, Object>, String> newEnvelope() {
        return new ValueEnvelope<>(
                new EnvelopeHeader<>("domain", "event", null, null, null, Map.of()),
                "msg",
                new HashMap<>(),
                new ArrayList<>()
        );
    }

    /** Evaluator that always succeeds and emits one set-event with value "<key>_val". */
    private static AspectExecutorAsync<EnrichmentWithDependencies,
            ValueEnvelope<Map<String, Object>, String>, List<CepEvent>> okEvaluator() {
        return (key, comp, input, cb) -> {
            String outKey = ((FakeEnricher) comp).output().get(0);
            cb.success(List.of(CepEvent.set(List.of(outKey), outKey + "_val")));
        };
    }

    @Test
    @DisplayName("single lane, single generation: all slots succeed and fold in slot order")
    void singleLaneSingleGenSuccess() throws Exception {
        var cepTc = new MapStringObjectCepStateTypeClass();

        // Build generation with two enrichers (use Set<EnrichmentWithDependencies> explicitly)
        Set<EnrichmentWithDependencies> gen = new LinkedHashSet<>();
        gen.add(new FakeEnricher("a"));
        gen.add(new FakeEnricher("b"));
        List<Set<EnrichmentWithDependencies>> gens = List.of(gen);

        var exec = new EnrichmentAspectExecutor<>(
                cepTc,
                gens,
                okEvaluator(),
                /* lanes */ 1,
                /* expectedPerSlot */ 2
        );

        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<ValueEnvelope<Map<String, Object>, String>> out = new AtomicReference<>();

        exec.call(0, newEnvelope(), new Callback<>() {
            @Override public void success(ValueEnvelope<Map<String, Object>, String> value) {
                out.set(value);
                done.countDown();
            }
            @Override public void failure(Throwable error) {
                fail("Unexpected failure: " + error);
            }
        });

        assertTrue(done.await(1, TimeUnit.SECONDS));
        Map<String, Object> state = out.get().cepState();
        // Values present and folded in slot order into the same map
        assertEquals("a_val", state.get("a")); // adjust assert values to match your evaluator output
        assertEquals("b_val", state.get("b"));
        assertEquals(2, out.get().cepStateModifications().size());
    }

    @Test
    @DisplayName("multiple generations: second generation sees first's updates")
    void multiGenerationFlow() throws Exception {
        var cepTc = new MapStringObjectCepStateTypeClass();
        Set<EnrichmentWithDependencies> g1 = new LinkedHashSet<>();
        g1.add(new FakeEnricher("a"));
        g1.add(new FakeEnricher("b"));
        Set<EnrichmentWithDependencies> g2 = new LinkedHashSet<>();
        g2.add(new FakeEnricher("c"));
        List<Set<EnrichmentWithDependencies>> gens = List.of(g1, g2);

        var exec = new EnrichmentAspectExecutor<>(
                cepTc, gens, okEvaluator(), 1, 2
        );

        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<ValueEnvelope<Map<String, Object>, String>> out = new AtomicReference<>();

        exec.call(0, newEnvelope(), new Callback<>() {
            @Override public void success(ValueEnvelope<Map<String, Object>, String> value) {
                out.set(value);
                done.countDown();
            }
            @Override public void failure(Throwable error) {
                fail("Unexpected failure: " + error);
            }
        });

        assertTrue(done.await(1, TimeUnit.SECONDS));
        Map<String, Object> state = out.get().cepState();
        // all three keys applied
        assertEquals(Set.of("a_val", "b_val", "c_val"),
                new HashSet<>(state.values()));
        assertEquals(3, out.get().cepStateModifications().size());
    }

    @Test
    @DisplayName("failure in a slot aborts its generation and surfaces EnrichmentBatchException")
    void failureAbortsGeneration() throws Exception {
        AspectExecutorAsync<EnrichmentWithDependencies,
                ValueEnvelope<Map<String, Object>, String>, List<CepEvent>> failingEval =
                (key, comp, input, cb) -> {
                    String k = ((FakeEnricher) comp).output().get(0);
                    if ("bad".equals(k)) {
                        cb.failure(new RuntimeException("boom"));
                    } else {
                        cb.success(List.of(CepEvent.set(List.of(k), k + "_ok")));
                    }
                };

        var cepTc = new MapStringObjectCepStateTypeClass();
        Set<EnrichmentWithDependencies> gen = new LinkedHashSet<>();
        gen.add(new FakeEnricher("good"));
        gen.add(new FakeEnricher("bad"));
        var gens = List.of(gen);

        var exec =  new EnrichmentAspectExecutor<>( // remove extra 'new' if already fixed
                cepTc, gens, failingEval, 1, 1
        );

        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> errRef = new AtomicReference<>();

        exec.call(0, newEnvelope(), new Callback<>() {
            @Override public void success(ValueEnvelope<Map<String, Object>, String> value) {
                fail("Should have failed");
            }
            @Override public void failure(Throwable error) {
                errRef.set(error);
                done.countDown();
            }
        });

        assertTrue(done.await(1, TimeUnit.SECONDS));
        assertTrue(errRef.get() instanceof EnrichmentBatchException);
        EnrichmentBatchException ex = (EnrichmentBatchException) errRef.get();
        assertEquals(0, ex.generationIndex());
        assertEquals(1, ex.errors().size());
        assertTrue(ex.errors().get(0).message().contains("boom"));
    }

    @Test
    @DisplayName("two lanes run independently")
    void lanesIndependent() throws Exception {
        var cepTc = new MapStringObjectCepStateTypeClass();
        Set<EnrichmentWithDependencies> gen = new LinkedHashSet<>();
        gen.add(new FakeEnricher("x"));
        var gens = List.of(gen);

        var exec = new EnrichmentAspectExecutor<>(
                cepTc, gens, okEvaluator(), 2, 1
        );

        CountDownLatch done = new CountDownLatch(2);
        AtomicReference<ValueEnvelope<Map<String, Object>, String>> out0 = new AtomicReference<>();
        AtomicReference<ValueEnvelope<Map<String, Object>, String>> out1 = new AtomicReference<>();

        exec.call(0, newEnvelope(), new Callback<ValueEnvelope<Map<String, Object>, String>>() {
            @Override public void success(ValueEnvelope<Map<String, Object>, String> value) {
                out0.set(value);
                done.countDown();
            }
            @Override public void failure(Throwable error) {
                fail("lane 0 failed: " + error);
            }
        });

        exec.call(1, newEnvelope(), new Callback<ValueEnvelope<Map<String, Object>, String>>() {
            @Override public void success(ValueEnvelope<Map<String, Object>, String> value) {
                out1.set(value);
                done.countDown();
            }
            @Override public void failure(Throwable error) {
                fail("lane 1 failed: " + error);
            }
        });

        assertTrue(done.await(1, TimeUnit.SECONDS));
        assertEquals("x_val", out0.get().cepState().get("x"));
        assertEquals("x_val", out1.get().cepState().get("x"));
        // distinct envelopes/states per lane
        assertNotSame(out0.get(), out1.get());
        assertNotSame(out0.get().cepState(), out1.get().cepState());
    }
}
