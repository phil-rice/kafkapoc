package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.common.function.SemigroupTc;
import com.hcltech.rmg.config.enrich.EnrichmentWithDependencies;
import com.hcltech.rmg.execution.aspects.GenerationResult;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that SlotCallBack forwards success / failure correctly into GenerationResult.
 */
@DisplayName("SlotCallBack → GenerationResult wiring")
class SlotCallBackTest {

    // No-op accumulator: just returns the same envelope (we don’t use folding here)
    private static final SemigroupTc<ValueEnvelope<Object, Object>, CepEvent> NOOP_ACC =
            (acc, evt) -> acc;

    /**
     * Minimal fake enricher implementing the real interface.
     */
    static final class FakeEnricher implements EnrichmentWithDependencies {
        private final String name;

        FakeEnricher(String name) {
            this.name = name;
        }

        @Override
        public List<List<String>> inputs() {
            return List.of(List.of("a", "b"));
        }

        @Override
        public List<String> output() {
            return List.of("out");
        }

        @Override
        public String toString() {
            return "FakeEnricher(" + name + ")";
        }
    }

    // ----------------------------------------------------------------------

    @Test
    @DisplayName("success() forwards epoch, slotIndex, and list by reference")
    void successForwards() {
        GenerationResult<ValueEnvelope<Object, Object>, CepEvent, EnrichmentWithDependencies> gr =
                new GenerationResult<>(NOOP_ACC, 8, 2, true);

        class Probe implements GenerationResult.SlotProbe<CepEvent, EnrichmentWithDependencies> {
            int seenEpoch = -1;
            int seenSlot = -1;
            CepEvent seenEvent;
            int successCount = 0;

            @Override
            public void onRecordSuccess(int epoch, int slot, CepEvent outOrNull) {
                this.seenEpoch = epoch;
                this.seenSlot = slot;
                this.seenEvent = outOrNull;
                this.successCount++;
            }
        }
        Probe probe = new Probe();
        gr.setProbe(probe);

        // Begin a run so runEpoch and CAS state are valid
        gr.beginRun(0, 8, null, acc -> {
        }, err -> {
        });
        int epoch = gr.currentEpoch();

        SlotCallBack<Object, Object> cb = new SlotCallBack<>(3)
                .init(epoch, gr, new FakeEnricher("E"));

        List<CepEvent> payload = new ArrayList<>();
        payload.add(null); // dummy event

        cb.success(payload);

        assertEquals(1, probe.successCount);
        assertEquals(epoch, probe.seenEpoch);
        assertEquals(3, probe.seenSlot);
        assertSame(payload, probe.seenEvent);
    }

    @Test
    @DisplayName("failure() forwards epoch, slotIndex, and enricher/error")
    void failureForwards() {
        GenerationResult<ValueEnvelope<Object, Object>, CepEvent, EnrichmentWithDependencies> gr =
                new GenerationResult<>(NOOP_ACC, 8, 2, true);

        class Probe implements GenerationResult.SlotProbe<CepEvent, EnrichmentWithDependencies> {
            int seenEpoch = -1;
            int seenSlot = -1;
            EnrichmentWithDependencies seenEnricher;
            Throwable seenError;
            int failureCount = 0;

            @Override
            public void onRecordFailure(int epoch, int slot, EnrichmentWithDependencies comp, Throwable error) {
                this.seenEpoch = epoch;
                this.seenSlot = slot;
                this.seenEnricher = comp;
                this.seenError = error;
                this.failureCount++;
            }
        }
        Probe probe = new Probe();
        gr.setProbe(probe);

        // Begin a run so runEpoch and CAS state are valid
        gr.beginRun(0, 8, null, acc -> {
        }, err -> {
        });
        int epoch = gr.currentEpoch();

        SlotCallBack<Object, Object> cb = new SlotCallBack<>(5)
                .init(epoch, gr, new FakeEnricher("bad"));

        RuntimeException boom = new RuntimeException("boom!");
        cb.failure(boom);

        assertEquals(1, probe.failureCount);
        assertEquals(epoch, probe.seenEpoch);
        assertEquals(5, probe.seenSlot);
        assertEquals("FakeEnricher(bad)", probe.seenEnricher.toString());
        assertSame(boom, probe.seenError);
    }

    @Test
    @DisplayName("duplicate success for same slot is ignored (probe not invoked twice)")
    void duplicateIgnored() {
        GenerationResult<ValueEnvelope<Object, Object>, CepEvent, EnrichmentWithDependencies> gr =
                new GenerationResult<>(NOOP_ACC, 8, 2, true);

        class Probe implements GenerationResult.SlotProbe<CepEvent, EnrichmentWithDependencies> {
            final AtomicInteger successes = new AtomicInteger();

            @Override
            public void onRecordSuccess(int epoch, int slot, CepEvent eventOrNull) {
                successes.incrementAndGet();
            }
        }
        Probe probe = new Probe();
        gr.setProbe(probe);

        // Begin a run so runEpoch and CAS state are valid
        gr.beginRun(0, 8, null, acc -> {
        }, err -> {
        });
        int epoch = gr.currentEpoch();

        SlotCallBack<Object, Object> cb = new SlotCallBack<>(2)
                .init(epoch, gr, new FakeEnricher("dup"));

        cb.success(Collections.singletonList(CepEvent.set(List.of("a"), 1)));
        cb.success(Collections.singletonList(CepEvent.set(List.of("b"), 2)));

        assertEquals(1, probe.successes.get(),
                "second duplicate success must not trigger probe");
    }
}
