package com.hcltech.rmg.common.async;

import com.hcltech.rmg.common.metrics.Metrics;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiConsumer;

public final class OrderPreservingAsyncExecutor<In, Out, FR>
        implements IOrderPreservingAsyncExecutor<In, Out, FR>,
        IOrderPreservingAsyncExecutorForTests<In, Out, FR> {

    private final OrderPreservingAsyncExecutorConfig<In, Out, FR> cfg;
    private final ILanes<In> lanes;
    private final PermitManager permits;
    private final IMpscRing<FR, In, Out> ring;   // FR is NOT stored in the ring
    private final FutureRecordTypeClass<FR, In, Out> frTypeClass;
    private final UserFnPort<In, Out, FR> userFn;
    private final Executor executor;
    private final Metrics metrics;               // <-- added

    private final boolean[] running;
    private final long timeoutNanos;
    private volatile boolean finished = false;

    private final int laneCount, laneMask;

    // Operator-thread-only drain context; cleared after each drain()
    private FR currentFr;
    private BiConsumer<In, Out> currentHook;

    private final IMpscRing.Handler<FR, In, Out> handler = new IMpscRing.Handler<>() {
        @Override
        public void onSuccess(FR _ignored, BiConsumer<In, Out> _ignored2,
                              In in, String corrId, Out out) {
            handleCompletion(currentFr, currentHook, in, corrId, out, null);
        }

        @Override
        public void onFailure(FR _ignored, BiConsumer<In, Out> _ignored2,
                              In in, String corrId, Throwable error) {
            handleCompletion(currentFr, currentHook, in, corrId, null, error);
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
        this(cfg, lanes, permits, ring, executor, frTypeClass, laneCount, userFn, Metrics.nullMetrics);
    }

    // Backward-compatible plus Metrics
    public OrderPreservingAsyncExecutor(
            OrderPreservingAsyncExecutorConfig<In, Out, FR> cfg,
            ILanes<In> lanes,
            PermitManager permits,
            IMpscRing<FR, In, Out> ring,
            Executor executor,
            FutureRecordTypeClass<FR, In, Out> frTypeClass,
            int laneCount,
            UserFnPort<In, Out, FR> userFn,
            Metrics metrics
    ) {
        this.cfg = Objects.requireNonNull(cfg);
        this.lanes = Objects.requireNonNull(lanes);
        this.permits = Objects.requireNonNull(permits);
        this.ring = Objects.requireNonNull(ring);
        this.executor = Objects.requireNonNull(executor);
        this.frTypeClass = Objects.requireNonNull(frTypeClass);
        this.userFn = Objects.requireNonNull(userFn);
        this.metrics = metrics == null ? Metrics.nullMetrics : metrics;

        if (laneCount <= 0 || (laneCount & (laneCount - 1)) != 0)
            throw new IllegalArgumentException("laneCount must be power of two > 0");

        this.laneCount = laneCount;
        this.laneMask = laneCount - 1;
        this.running = new boolean[laneCount];
        this.timeoutNanos = cfg.timeoutMillis() <= 0 ? 0L : cfg.timeoutMillis() * 1_000_000L;
    }

    // ===================================================================================

    @Override
    public void add(In input, FR fr, BiConsumer<In, Out> hook) {
        while (!finished) {
            // cooperative drain on operator thread with CURRENT fr/hook
            drain(fr, hook);
            if (internalTryAndAdd(input, fr, hook)) return;
        }
    }

    @Override
    public void close() {
        finished = true;
    }

    /**
     * Drain completions on the operator thread using the CURRENT fr + hook.
     * Context is set for this call and cleared immediately after.
     */
    public void drain(final FR fr, final BiConsumer<In, Out> hook) {
        if (finished) return;
        this.currentFr = fr;
        this.currentHook = hook;
        try {
            metrics.increment("async.drain.calls");
            ring.drain((a, b) -> {
            }, handler);
        } finally {
            this.currentFr = null;
            this.currentHook = null;
        }
    }

    // ===================================================================================

    private boolean internalTryAndAdd(In in, FR fr, BiConsumer<In, Out> hook) {
        final ILane<In> lane = lanes.lane(Objects.requireNonNull(in));
        final int idx = indexFrom(in);

        if (lane.isFull()) {
            if (!maybeTimeoutHead(fr, hook, idx, lane)) return false;
        }

        final long now = cfg.timeService().currentTimeNanos();
        final String corrId = cfg.correlator().correlationId(in);

        lane.enqueue(in, corrId, now);

        if (!running[idx] && permits.tryAcquire()) {
            running[idx] = true;
            submitHead(idx, lane); // offload actual work via executor
        }
        return true;
    }

    /**
     * Evict head on timeout during admission. Silent drop (design).
     * The eventual completion will be ignored if it's no longer the head.
     */
    private boolean maybeTimeoutHead(FR fr,
                                     BiConsumer<In, Out> hook,
                                     int laneIdx,
                                     ILane<In> lane) {
        if (timeoutNanos <= 0) return false;

        final long now = cfg.timeService().currentTimeNanos();
        final long headStarted = lane.headStartedAtNanos();
        if ((now - headStarted) < timeoutNanos) return false;

        metrics.increment("async.evict.timeout");
        lane.popHead(ignored -> { /* deliberate no-op */ });

        if (!lane.isEmpty()) submitHead(laneIdx, lane);
        else markIdleAndRelease(laneIdx);

        return true;
    }

    /**
     * Completion path: ONLY commit when (in,corrId) matches the lane head.
     * Otherwise (stale/mismatch) ignore.
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

        // stale or re-ordered (not head) -> ignore
        if (lane.isEmpty() || lane.headT() != in || !lane.headCorrId().equals(corrId)) return;

        // Pop head (we are the head)
        lane.popHead(ignored -> {
        });

        if (error == null) {
            cfg.futureRecord().completed(fr, hook, in, out);
            metrics.increment("async.complete.ok");
        } else {
            cfg.futureRecord().failed(fr, hook, in, error);
            metrics.increment("async.complete.fail");
        }

        if (!lane.isEmpty()) submitHead(idx, lane);
        else markIdleAndRelease(idx);
    }

    private final UserFnPort.Completion<In, Out> submitCompletion = new UserFnPort.Completion<>() {
        @Override
        public void success(In in, String corrId, Out out) {
            metrics.increment("async.ring.offer.success");
            ring.offerSuccess(in, corrId, out);
        }

        @Override
        public void failure(In in, String corrId, Throwable err) {
            metrics.increment("async.ring.offer.failure");
            ring.offerFailure(in, corrId, err);
        }
    };

    /**
     * Submit current lane head by offloading to the managed executor.
     * The userFn is invoked on a pool thread; results are pushed to the MPSC ring.
     */
    private void submitHead(int laneIdx, ILane<In> lane) {
        final In headIn = lane.headT();
        final String corr = lane.headCorrId();

        try {
            metrics.increment("async.submit.head");
            executor.execute(new SubmitTask<>(userFn, frTypeClass, laneIdx, headIn, corr, submitCompletion, ring));
        } catch (RejectedExecutionException rex) {
            metrics.increment("async.submit.rejected");
            ring.offerFailure(headIn, corr, rex);
        } catch (Throwable submitError) {
            metrics.increment("async.submit.error");
            ring.offerFailure(headIn, corr, submitError);
        }
    }

    private void markIdleAndRelease(int laneIdx) {
        if (running[laneIdx]) {
            running[laneIdx] = false;
            permits.release();
            metrics.increment("async.lane.idle");
        }
    }

    private int indexFrom(In in) {
        return cfg.correlator().laneHash(in) & laneMask;
    }

    // ---- Diagnostics (operator thread) ----------------------------------

    /**
     * Cheap snapshot of lane health for tests/alerts.
     */
    public LaneDiagnostics laneDiagnostics() {
        int empty = 0, full = 0, inUse = 0, expired = 0;
        final long now = cfg.timeService().currentTimeNanos();
        final long to = timeoutNanos;

        // Use existing test hooks if present (Lanes implements ILanesTestHooks)
        if (lanes instanceof ILanesTestHooks<?> thRaw) {
            @SuppressWarnings("unchecked")
            ILanesTestHooks<In> th = (ILanesTestHooks<In>) thRaw;
            int count = th._laneCount();
            for (int i = 0; i < count; i++) {
                ILane<In> l = th._laneAt(i);
                if (l.isEmpty()) empty++;
                else if (l.isFull()) full++;
                else inUse++;
                if (to > 0 && !l.isEmpty() && now - l.headStartedAtNanos() >= to) expired++;
            }
            // optional: emit snapshot as histograms
            metrics.histogram("async.lane.empty", empty);
            metrics.histogram("async.lane.full", full);
            metrics.histogram("async.lane.inUse", inUse);
            metrics.histogram("async.lane.expired", expired);
            return new LaneDiagnostics(count, empty, full, inUse, expired, now, to);
        }

        // Fallback: no introspection available
        return new LaneDiagnostics(laneCount, -1, -1, -1, -1, now, to);
    }

    // ---------------------------------------------------------------------
    // Async user function port — no FR/hook (off-thread)
    // ---------------------------------------------------------------------
    public interface UserFnPort<I, O, FR> {
        void submit(FutureRecordTypeClass<FR, I, O> tc,
                    int laneId,
                    I in,
                    String corrId,
                    Completion<I, O> completion);

        interface Completion<I, O> {
            void success(I in, String corrId, O out);

            void failure(I in, String corrId, Throwable err);
        }
    }

    @Override
    public ILanes<In> lanes() {
        return lanes;
    }

    // Small runnable to avoid capturing lambdas in hot path
    private static final class SubmitTask<I, O, FRX> implements Runnable {
        private final UserFnPort<I, O, FRX> userFn;
        private final FutureRecordTypeClass<FRX, I, O> tc;
        private final I in;
        private final String corr;
        private final UserFnPort.Completion<I, O> completion;
        private final IMpscRing<FRX, I, O> ring;
        private final int laneId;

        SubmitTask(UserFnPort<I, O, FRX> userFn,
                   FutureRecordTypeClass<FRX, I, O> tc,
                   int laneId,
                   I in,
                   String corr,
                   UserFnPort.Completion<I, O> completion,
                   IMpscRing<FRX, I, O> ring) {
            this.userFn = userFn;
            this.tc = tc;
            this.laneId = laneId;
            this.in = in;
            this.corr = corr;
            this.completion = completion;
            this.ring = ring;
        }

        @Override
        public void run() {
            try {
                userFn.submit(tc, laneId, in, corr, completion);
            } catch (Throwable t) {
                ring.offerFailure(in, corr, t);
            }
        }
    }
}
