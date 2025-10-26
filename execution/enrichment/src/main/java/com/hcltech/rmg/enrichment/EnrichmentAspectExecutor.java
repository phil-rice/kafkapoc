package com.hcltech.rmg.enrichment;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.common.function.Callback;
import com.hcltech.rmg.common.function.SemigroupTc;
import com.hcltech.rmg.config.enrich.EnrichmentWithDependencies;
import com.hcltech.rmg.execution.aspects.AspectExecutorAsync;
import com.hcltech.rmg.execution.aspects.GenerationResult;
import com.hcltech.rmg.messages.ValueEnvelope;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Async-shaped, lane-aware enrichment orchestrator.
 * Executes a topologically-ordered list of dependency-free enrichment generations
 * over a {@link ValueEnvelope}, one invocation per lane at a time.
 * <p>
 * Guarantees:
 * • per-slot idempotence (duplicates ignored via CAS),
 * • exactly-once completion per generation (single onComplete),
 * • no per-call allocations on the hot path (callbacks are preallocated per lane/slot).
 *
 * @param <CepState> CEP state type carried in the {@link ValueEnvelope}
 * @param <Msg>      message payload type carried in the {@link ValueEnvelope}
 */
public final class EnrichmentAspectExecutor<CepState, Msg>
        implements IEnrichmentAspectExecutor<CepState, Msg> {

    // ----- Fixed dependencies -----
    private final CepStateTypeClass<CepState> cepTc;
    /**
     * Topologically sorted; each Set is a dependency-free generation with stable iteration order.
     */
    private final List<Set<EnrichmentWithDependencies>> generations;
    /**
     * Async evaluator for a single enrichment node; returns null or {@link CepEvent}.
     */
    private final AspectExecutorAsync<EnrichmentWithDependencies, ValueEnvelope<CepState, Msg>, CepEvent> evaluator;
    /**
     * Folds a single event into the envelope (typically ValueEnvelope::withNewCepEvent).
     */
    private final SemigroupTc<ValueEnvelope<CepState, Msg>, CepEvent> accTc;

    // ----- Per-lane preallocated state -----
    private static final class LaneState<CepStateT, MsgT> {
        final GenerationResult<ValueEnvelope<CepStateT, MsgT>, CepEvent, EnrichmentWithDependencies> gr;
        /**
         * One reusable callback per slot in the largest generation.
         */
        final SlotCb<CepStateT, MsgT>[] cbs;

        @SuppressWarnings("unchecked")
        LaneState(int maxSlots,
                  int expectedPerSlot,
                  SemigroupTc<ValueEnvelope<CepStateT, MsgT>, ?> accTc) {
            this.gr = new GenerationResult<>(
                    (SemigroupTc<ValueEnvelope<CepStateT, MsgT>, CepEvent>) accTc,
                    maxSlots,
                    expectedPerSlot
            );
            this.cbs = (SlotCb<CepStateT, MsgT>[]) new SlotCb[maxSlots];
            for (int i = 0; i < maxSlots; i++) {
                this.cbs[i] = new SlotCb<>();
            }
        }
    }

    /**
     * Reusable per-slot adapter from {@code Callback<List<CepEvent>>} to {@link GenerationResult} recording.
     * Carries per-run context via {@link #init}.
     */
    private static final class SlotCb<CepStateT, MsgT> implements Callback<CepEvent> {
        private int epoch;
        private int slot;
        private GenerationResult<ValueEnvelope<CepStateT, MsgT>, CepEvent, EnrichmentWithDependencies> gr;
        private EnrichmentWithDependencies comp;

        void init(int epoch,
                  int slot,
                  GenerationResult<ValueEnvelope<CepStateT, MsgT>, CepEvent, EnrichmentWithDependencies> gr,
                  EnrichmentWithDependencies comp) {
            this.epoch = epoch;
            this.slot = slot;
            this.gr = gr;
            this.comp = comp;
        }

        @Override
        public void success(final CepEvent eventOrNull) {
            gr.recordSuccess(epoch, slot, eventOrNull);
        }

        @Override
        public void failure(final Throwable error) {
            gr.recordFailure(epoch, slot, comp, error);
        }
    }

    private final LaneState<CepState, Msg>[] lanes;

    @SuppressWarnings("unchecked")
    public EnrichmentAspectExecutor(CepStateTypeClass<CepState> cepTc,
                                    List<Set<EnrichmentWithDependencies>> generations,
                                    AspectExecutorAsync<EnrichmentWithDependencies, ValueEnvelope<CepState, Msg>, CepEvent> evaluator,
                                    int laneCount,
                                    int expectedEventsPerSlot) {
        this.cepTc = cepTc;
        this.generations = generations;
        this.evaluator = evaluator;
        this.accTc = (env, evt) -> env.withNewCepEvent(cepTc, evt);

        final int maxSlots = maxSlots(generations);
        int lanesSize = Math.max(1, laneCount);
        this.lanes = (LaneState<CepState, Msg>[]) new LaneState[lanesSize];
        for (int i = 0; i < lanesSize; i++) {
            this.lanes[i] = new LaneState<>(maxSlots, Math.max(1, expectedEventsPerSlot), this.accTc);
        }
    }

    // ---- Public async entrypoint (lane-aware) ----
    @Override
    public void call(final int laneId,
                     final ValueEnvelope<CepState, Msg> input,
                     final Callback<? super ValueEnvelope<CepState, Msg>> cb) {
        if (generations == null || generations.isEmpty()) {
            cb.success(input);
            return;
        }
        final var lane = lanes[laneId];
        runGeneration(lane, 0, input, cb);
    }

    // ---- Orchestration per lane ----
    private void runGeneration(final LaneState<CepState, Msg> lane,
                               final int genIdx,
                               final ValueEnvelope<CepState, Msg> accIn,
                               final Callback<? super ValueEnvelope<CepState, Msg>> cb) {
        if (genIdx >= generations.size()) {
            cb.success(accIn);
            return;
        }

        final Set<EnrichmentWithDependencies> gen = generations.get(genIdx);
        if (gen == null || gen.isEmpty()) {
            // Nothing to do in this generation → advance
            runGeneration(lane, genIdx + 1, accIn, cb);
            return;
        }

        final EnrichmentWithDependencies[] slotComps = gen.toArray(new EnrichmentWithDependencies[0]);
        final int n = slotComps.length;

        final Consumer<ValueEnvelope<CepState, Msg>> onGenSuccess =
                nextAcc -> runGeneration(lane, genIdx + 1, nextAcc, cb);

        // NOTE: GenerationResult already aggregates per-slot failures and constructs EnrichmentBatchException.
        final Consumer<Throwable> onGenFailure = cb::failure;

        lane.gr.beginRun(genIdx, n, accIn, onGenSuccess, onGenFailure);
        final int ep = lane.gr.currentEpoch();

        for (int slot = 0; slot < n; ++slot) {
            final EnrichmentWithDependencies comp = slotComps[slot];
            final SlotCb<CepState, Msg> slotCb = lane.cbs[slot];
            slotCb.init(ep,
                    slot,
                    lane.gr,
                    comp);
            final String key = "enrichment:" + genIdx + ":" + slot;
            try {
                // Start the single enricher asynchronously; cb is reused per slot (no allocation).
                evaluator.call(key, comp, accIn, slotCb);
            } catch (Throwable t) {
                // Misbehaving enricher threw before invoking its callback → mark slot failed.
                // GenerationResult will aggregate and trigger onFailure exactly once.
                lane.gr.recordFailure(ep, slot, comp, t);
            }
        }
    }

    private static int maxSlots(List<? extends Set<?>> gens) {
        int m = 1;
        if (gens != null) {
            for (Set<?> g : gens) {
                if (g != null && g.size() > m) {
                    m = g.size();
                }
            }
        }
        return m;
    }
}
