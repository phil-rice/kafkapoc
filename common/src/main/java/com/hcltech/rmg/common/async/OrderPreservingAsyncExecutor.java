package com.hcltech.rmg.common.async;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public final class OrderPreservingAsyncExecutor<In, Out, FR>
        implements IOrderPreservingAsyncExecutor<In, Out, FR>,
        IOrderPreservingAsyncExecutorForTests<In, Out, FR> {

    private final OrderPreservingAsyncExecutorConfig<In, Out, FR> cfg;
    private final ILanes<In> lanes;
    private final PermitManager permits;
    private final IMpscRing<FR, In, Out> ring;   // FR is NOT stored in the ring
    private final Executor executor;
    private final FutureRecordTypeClass<FR, In, Out> frTypeClass;
    private final UserFnPort<In, Out, FR> userFn;

    // single source of truth for lane indexing (avoid mismatch with lanes)
    private final Correlator<In> correlator;

    private final boolean[] running;
    private final long timeoutNanos;
    private volatile boolean finished = false;

    private final int laneCount, laneMask;

    // Ensure single-consumer semantics for the MPSC ring drain
    private final AtomicBoolean draining = new AtomicBoolean(false);

    // ---------------------------------------------------------------------
    // Allocation-free drain support: reuse a single commit + handler per executor
    // ---------------------------------------------------------------------

    /** Context set at the start of drain(fr, hook); only read by the shared handler during that call. */
    private FR drainFrCtx;
    private BiConsumer<In, Out> drainHookCtx;

    /** Reusable no-op commit for ring.drain(..). The ring’s commit expects BiConsumer<In,Out>. */
    private final BiConsumer<In, Out> commitNoop = (a, b) -> { /* no-op */ };

    /** Preallocated handler reused on every drain(..) call (no per-call allocations). */
    private final IMpscRing.Handler<FR, In, Out> sharedDrainHandler = new IMpscRing.Handler<>() {
        @Override
        public void onSuccess(FR _ignored,
                              BiConsumer<In, Out> _ignored2,
                              In in,
                              String corrId,
                              Out out) {
            handleCompletion(drainFrCtx, drainHookCtx, in, corrId, out, null);
        }

        @Override
        public void onFailure(FR _ignored,
                              BiConsumer<In, Out> _ignored2,
                              In in,
                              String corrId,
                              Throwable error) {
            handleCompletion(drainFrCtx, drainHookCtx, in, corrId, null, error);
        }
    };

    public OrderPreservingAsyncExecutor(
            OrderPreservingAsyncExecutorConfig<In, Out, FR> cfg,
            ILanes<In> lanes,
            PermitManager permits,
            IMpscRing<FR, In, Out> ring,
            Executor executor,
            FutureRecordTypeClass<FR, In, Out> frTypeClass,
            int laneCount,
            UserFnPort<In, Out, FR> userFn
    ) {
        this.cfg = Objects.requireNonNull(cfg);
        this.lanes = Objects.requireNonNull(lanes);
        this.permits = Objects.requireNonNull(permits);
        this.ring = Objects.requireNonNull(ring);
        this.executor = Objects.requireNonNull(executor);
        this.frTypeClass = Objects.requireNonNull(frTypeClass);
        this.userFn = Objects.requireNonNull(userFn);

        if (laneCount <= 0 || (laneCount & (laneCount - 1)) != 0)
            throw new IllegalArgumentException("laneCount must be power of two > 0");

        this.laneCount = laneCount;
        this.laneMask = laneCount - 1;
        this.running = new boolean[laneCount];
        this.timeoutNanos = cfg.timeoutMillis() <= 0 ? 0L : cfg.timeoutMillis() * 1_000_000L;

        // unify index math to a single correlator (avoid split-brain lane vs. index)
        this.correlator = Objects.requireNonNull(cfg.correlator(), "cfg.correlator");
    }

    // ===================================================================================

    @Override
    public void add(In input, FR fr, BiConsumer<In, Out> hook) {
        while (!finished) {
            // Cooperative drain with the CURRENT fr/hook (operator thread).
            // This may be called concurrently with an external operator thread.
            // The drain() method ensures single-consumer semantics.
            drain(fr, hook);
            if (internalTryAndAdd(input, fr, hook)) return;
        }
    }

    @Override
    public void close() {
        finished = true;
    }

    /**
     * Drain completions; apply CURRENT fr + hook on the operator thread.
     * Allocation-free: reuses a preallocated commit callback and handler.
     *
     * IMPORTANT: This method now enforces **single-consumer** semantics with a non-blocking guard.
     * If another thread is draining, this call returns immediately.
     */
    @Override
    public void drain(final FR fr, final BiConsumer<In, Out> hook) {
        if (finished) return;

        // Ensure only one thread drains the MPSC ring at a time.
        if (!draining.compareAndSet(false, true)) {
            return; // another thread is the active consumer right now
        }

        try {
            // publish current context for the shared handler only after acquiring the guard
            this.drainFrCtx   = fr;
            this.drainHookCtx = hook;

            // Non-blocking drain with reusable handler (no per-call allocations)
            ring.drain(commitNoop, sharedDrainHandler);
        } finally {
            draining.set(false);
        }
    }

    // ===================================================================================

    private boolean internalTryAndAdd(In in, FR fr, BiConsumer<In, Out> hook) {
        final ILane<In> lane = lanes.lane(Objects.requireNonNull(in));
        final int idx = indexFrom(in); // computed from the *same* correlator as lanes

        if (lane.isFull()) {
            if (!maybeTimeoutHead(fr, hook, idx, lane)) return false;
        }

        final long now = cfg.timeService().currentTimeNanos();
        final String corrId = correlator.correlationId(in);

        lane.enqueue(in, corrId, now);

        if (!running[idx] && permits.tryAcquire()) {
            running[idx] = true;
            submitHead(idx, lane); // submit head for this lane
        }
        return true;
    }

    /**
     * Evict head on timeout during admission using CURRENT fr + hook.
     */
    private boolean maybeTimeoutHead(FR fr,
                                     BiConsumer<In, Out> hook,
                                     int laneIdx,
                                     ILane<In> lane) {
        if (timeoutNanos <= 0) return false;

        final long now = cfg.timeService().currentTimeNanos();
        final long headStarted = lane.headStartedAtNanos();
        if ((now - headStarted) < timeoutNanos) return false;

        final In headIn = lane.headT();
        final long elapsed = now - headStarted;

        // Pop head; apply timeout on operator thread with CURRENT fr/hook
        lane.popHead(ignored ->
                cfg.futureRecord().timedOut(fr, hook, headIn, elapsed)
        );

        if (!lane.isEmpty()) submitHead(laneIdx, lane);
        else markIdleAndRelease(laneIdx);

        return true;
    }

    /**
     * Completion path: commit with CURRENT fr + hook (from this drain call), then continue.
     *
     * Semantics per your direction:
     * - If the lane head doesn't match the completion (in/corrId), we ONLY drop it
     *   when we can prove the current head has already surpassed the timeout.
     * - Otherwise we record a failure (so accounting/latches never hang).
     */
    private void handleCompletion(FR fr,
                                  BiConsumer<In, Out> hook,
                                  In in,
                                  String corrId,
                                  Out out,
                                  Throwable error) {
        if (finished) return;

        final ILane<In> lane = lanes.lane(in);
        final int idx = indexFrom(in);

        // If the head doesn't match, decide drop-vs-fail based on timeout proof.
        if (lane.isEmpty()
                || lane.headT() != in
                || !Objects.equals(lane.headCorrId(), corrId)) {

            // If we can prove the CURRENT head is overdue, treat as already timed out and drop.
            if (timeoutNanos > 0 && !lane.isEmpty()) {
                final long now = cfg.timeService().currentTimeNanos();
                final long elapsed = now - lane.headStartedAtNanos();
                if (elapsed >= timeoutNanos) {
                    return; // allowed silent drop: current head has timed out by policy
                }
            }

            // Otherwise treat as a failure to avoid silent loss / stuck latches
            cfg.futureRecord().failed(fr, hook, in,
                    new IllegalStateException("Completion mismatch without timeout: "
                            + "laneEmpty=" + lane.isEmpty()
                            + ", corrId=" + corrId
                            + ", expectedHead=" + lane.headT()
                            + ", expectedCorrId=" + (lane.isEmpty() ? null : lane.headCorrId())));
            return;
        }

        // Pop current head (it matches completion)
        lane.popHead(ignored -> {});

        if (error == null) cfg.futureRecord().completed(fr, hook, in, out);
        else cfg.futureRecord().failed(fr, hook, in, error);

        if (!lane.isEmpty()) submitHead(idx, lane);
        else markIdleAndRelease(idx);
    }

    private final UserFnPort.Completion<In, Out> submitCompletion = new UserFnPort.Completion<>() {
        @Override
        public void success(In in, String corrId, Out out) {
            ring.offerSuccess(in, corrId, out);
        }

        @Override
        public void failure(In in, String corrId, Throwable err) {
            ring.offerFailure(in, corrId, err);
        }
    };

    /**
     * Submit current head; userFn only gets in+corrId+completion (no FR/hook).
     */
    private void submitHead(int laneIdx, ILane<In> lane) {
        final In headIn = lane.headT();
        final String corr = lane.headCorrId();

        try {
            userFn.submit(frTypeClass, headIn, corr, submitCompletion);
        } catch (Throwable submitError) {
            ring.offerFailure(headIn, corr, submitError);
        }
    }

    private void markIdleAndRelease(int laneIdx) {
        if (running[laneIdx]) {
            running[laneIdx] = false;
            permits.release();
        }
    }

    private int indexFrom(In in) {
        return correlator.laneHash(in) & laneMask;
    }

    // ---------------------------------------------------------------------
    // Diagnostics (snapshot-on-demand; cheap and safe for stress tests)
    // ---------------------------------------------------------------------

    /** Immutable snapshot of executor state focused on lane occupancy & permits. */
    public static final class Diagnostics {
        public final int laneCount;
        public final int emptyLanes;
        public final int fullLanes;
        public final int partialLanes;
        public final int runningLanes;
        public final int permitsAvailable;
        public final int permitsMax;
        /** Top-N busiest lanes by size: [laneIndex:size] */
        public final java.util.List<String> busiestLanesSample;

        Diagnostics(int laneCount, int empty, int full, int partial,
                    int running, int avail, int max, java.util.List<String> sample) {
            this.laneCount = laneCount;
            this.emptyLanes = empty;
            this.fullLanes = full;
            this.partialLanes = partial;
            this.runningLanes = running;
            this.permitsAvailable = avail;
            this.permitsMax = max;
            this.busiestLanesSample = sample;
        }

        @Override
        public String toString() {
            return "Diagnostics{" +
                    "laneCount=" + laneCount +
                    ", emptyLanes=" + emptyLanes +
                    ", fullLanes=" + fullLanes +
                    ", partialLanes=" + partialLanes +
                    ", runningLanes=" + runningLanes +
                    ", permitsAvailable=" + permitsAvailable +
                    ", permitsMax=" + permitsMax +
                    ", busiestLanesSample=" + busiestLanesSample +
                    '}';
        }
    }

    @SuppressWarnings("unchecked")
    public Diagnostics diagnostics() {
        int empty = 0, full = 0, partial = 0, runningCnt = 0;

        class LaneStat { int idx; int size; int depth; }
        java.util.List<LaneStat> stats = new java.util.ArrayList<>(Math.min(32, laneCount));

        ILanesTestHooks<In> lanesHooks = (ILanesTestHooks<In>) this.lanes;
        for (int i = 0; i < laneCount; i++) {
            ILane<In> lane = lanesHooks._laneAt(i);
            ILaneTestHooks<In> laneHooks = (ILaneTestHooks<In>) lane;
            int size = laneHooks._countForTest();
            int depth = laneHooks._depthForTest();

            if (size == 0) empty++;
            else if (size == depth) full++;
            else partial++;

            if (running[i]) runningCnt++;

            if (stats.size() < 16 || size > stats.get(stats.size() - 1).size) {
                LaneStat ls = new LaneStat();
                ls.idx = i; ls.size = size; ls.depth = depth;
                stats.add(ls);
                stats.sort(java.util.Comparator.comparingInt((LaneStat s) -> s.size).reversed());
                if (stats.size() > 16) stats.remove(stats.size() - 1);
            }
        }

        java.util.List<String> sample = new java.util.ArrayList<>(stats.size());
        for (LaneStat s : stats) {
            sample.add("[" + s.idx + ":" + s.size + "/" + s.depth + "]");
        }

        int avail = permits.availablePermits();
        int max = permits.maxPermits();

        return new Diagnostics(laneCount, empty, full, partial, runningCnt, avail, max, sample);
    }

    // ---------------------------------------------------------------------
    // Async user function port — no FR/hook (off-thread)
    // ---------------------------------------------------------------------
    public interface UserFnPort<I, O, FR> {
        void submit(FutureRecordTypeClass<FR, I, O> tc,
                    I in,
                    String corrId,
                    Completion<I, O> completion);

        interface Completion<I, O> {
            void success(I in, String corrId, O out);
            void failure(I in, String corrId, Throwable err);
        }
    }

    @Override
    public ILanes<In> lanes() { return lanes; }

    @Override
    public Executor executor() { return executor; }
}
