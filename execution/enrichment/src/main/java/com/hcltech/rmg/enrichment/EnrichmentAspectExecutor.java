package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.common.function.Callback;
import com.hcltech.rmg.config.enrich.EnrichmentWithDependencies;
import com.hcltech.rmg.execution.aspects.AspectExecutorAsync;
import com.hcltech.rmg.messages.ValueEnvelope;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public final class EnrichmentAspectExecutor<CepState, Msg>
        implements IEnrichmentAspectExecutor<CepState, Msg> {

    private final CepStateTypeClass<CepState> cepStateTypeClass;
    /** Topologically sorted; each Set is a dependency-free generation with stable iteration order. */
    private final List<Set<EnrichmentWithDependencies>> generations;
    private final AspectExecutorAsync<EnrichmentWithDependencies,
            ValueEnvelope<CepState, Msg>,
            CepEvent> evaluator;

    public EnrichmentAspectExecutor(
            CepStateTypeClass<CepState> cepStateTypeClass,
            List<Set<EnrichmentWithDependencies>> generations,
            AspectExecutorAsync<EnrichmentWithDependencies,
                    ValueEnvelope<CepState, Msg>,
                    CepEvent> evaluator) {
        this.cepStateTypeClass = cepStateTypeClass;
        this.generations = generations;
        this.evaluator = evaluator;
    }

    @Override
    public void call(final ValueEnvelope<CepState, Msg> in,
                     final Callback<? super ValueEnvelope<CepState, Msg>> cb) {
        if (generations == null || generations.isEmpty()) {
            cb.success(in);
            return;
        }
        runGeneration(0, in, cb);
    }

    private void runGeneration(final int genIdx,
                               final ValueEnvelope<CepState, Msg> accIn,
                               final Callback<? super ValueEnvelope<CepState, Msg>> cb) {
        if (genIdx >= generations.size()) {
            cb.success(accIn);
            return;
        }

        final Set<EnrichmentWithDependencies> gen = generations.get(genIdx);
        if (gen == null || gen.isEmpty()) {
            runGeneration(genIdx + 1, accIn, cb);
            return;
        }

        final int n = gen.size();
        final CepEvent[] results = new CepEvent[n];
        final Failure[]  failures = new Failure[n];
        final Counter counter = new Counter(n);

        int slot = 0;
        for (EnrichmentWithDependencies enricher : gen) {
            final int idx = slot++;
            evaluator.call(
                    "enrichment:" + genIdx + ":" + idx,
                    enricher,
                    accIn,
                    new Callback<>() {
                        @Override public void success(CepEvent evt) {
                            results[idx] = evt; // evt may be null (no-op)
                            if (counter.decAndIsZero()) onGenerationDone(genIdx, accIn, n, results, failures, cb);
                        }
                        @Override public void failure(Throwable error) {
                            failures[idx] = new Failure(enricher, error);
                            if (counter.decAndIsZero()) onGenerationDone(genIdx, accIn, n, results, failures, cb);
                        }
                    }
            );
        }
    }

    private void onGenerationDone(final int genIdx,
                                  final ValueEnvelope<CepState, Msg> accIn,
                                  final int n,
                                  final CepEvent[] results,
                                  final Failure[] failures,
                                  final Callback<? super ValueEnvelope<CepState, Msg>> cb) {
        // Aggregate failures
        List<Failure> errs = null;
        for (int i = 0; i < n; i++) {
            if (failures[i] != null) {
                if (errs == null) errs = new ArrayList<>();
                errs.add(failures[i]);
            }
        }
        if (errs != null) {
            cb.failure(new EnrichmentBatchException(genIdx, errs));
            return;
        }

        // Deterministic fold by stable set iteration order → same slot order we used above (0..n-1)
        ValueEnvelope<CepState, Msg> acc = accIn;
        for (int i = 0; i < n; i++) {
            final CepEvent evt = results[i];
            if (evt != null) acc = acc.withNewCepEvent(cepStateTypeClass, evt);
        }
        runGeneration(genIdx + 1, acc, cb);
    }

    private static final class Counter {
        private int remaining;
        Counter(int n) { this.remaining = n; }
        boolean decAndIsZero() { return --remaining == 0; }
    }

    public static final class Failure {
        public final EnrichmentWithDependencies enricher;
        public final Throwable error;
        public Failure(EnrichmentWithDependencies enricher, Throwable error) {
            this.enricher = enricher; this.error = error;
        }
    }

    public static final class EnrichmentBatchException extends RuntimeException {
        private final int generationIndex;
        private final List<Failure> failures;
        public EnrichmentBatchException(int generationIndex, List<Failure> failures) {
            super("Enrichment generation " + generationIndex + " failed with " + failures.size() + " error(s).");
            this.generationIndex = generationIndex;
            this.failures = failures;
        }
        public int generationIndex() { return generationIndex; }
        public List<Failure> failures() { return failures; }
    }
}
