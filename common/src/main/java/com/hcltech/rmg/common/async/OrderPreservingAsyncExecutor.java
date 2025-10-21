package com.hcltech.rmg.common.async;

import java.util.Objects;
import java.util.concurrent.Executor;

public final class OrderPreservingAsyncExecutor<In, Out, FR>
        implements IOrderPreservingAsyncExecutor<In, Out, FR>,
        IOrderPreservingAsyncExecutorForTests<In, Out, FR> {

    private final OrderPreservingAsyncExecutorConfig<In, Out, FR> cfg;
    private final ILanes<In> lanes;
    private final PermitManager permits;
    private final IMpscRing<FR, In, Out> ring;
    private final Executor executor;
    private final FutureRecordTypeClass<FR, In, Out> frTypeClass; // (kept if you use it elsewhere)
    private final UserFnPort<In, Out, FR> userFn;

    private final boolean[] running;
    private final long timeoutNanos;
    private volatile boolean finished = false;

    // lane geometry
    private final int laneCount, laneMask, laneDepth, depthMask;

    // Per-lane FR shadow rings (arrays) — no allocations at runtime
    private final Object[][] frRings; // [laneIdx][slot] holds FR
    private final int[] frHeadIdx, frTailIdx, frCount;

    // Ring handler: FR arrives from ring
    private final IMpscRing.Handler<FR, In, Out> ringHandler = new IMpscRing.Handler<>() {
        @Override
        public void onSuccess(FR fr, In in, String corrId, Out out) {
            handleCompletion(fr, in, corrId, out, null);
        }

        @Override
        public void onFailure(FR fr, In in, String corrId, Throwable error) {
            handleCompletion(fr, in, corrId, null, error);
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
        this.frTypeClass = frTypeClass;
        this.userFn = Objects.requireNonNull(userFn);

        if (laneCount <= 0 || (laneCount & (laneCount - 1)) != 0)
            throw new IllegalArgumentException("laneCount must be power of two > 0");
        if (cfg.laneDepth() <= 0 || (cfg.laneDepth() & (cfg.laneDepth() - 1)) != 0)
            throw new IllegalArgumentException("laneDepth must be power of two > 0");

        this.laneCount = laneCount;
        this.laneMask = laneCount - 1;
        this.laneDepth = cfg.laneDepth();
        this.depthMask = laneDepth - 1;

        this.running = new boolean[laneCount];
        this.timeoutNanos = cfg.timeoutMillis() <= 0 ? 0L : cfg.timeoutMillis() * 1_000_000L;

        // init FR rings
        this.frRings = new Object[laneCount][laneDepth];
        this.frHeadIdx = new int[laneCount];
        this.frTailIdx = new int[laneCount];
        this.frCount = new int[laneCount];
    }

    @Override
    public void add(In input, FR fr) {
        while (!finished) {
            drain(fr); // FR carried per event; param here is just for API symmetry
            if (internalTryAndAdd(input, fr)) return;
        }
    }

    @Override
    public void close() {
        finished = true;
    }

    @Override
    public void drain(FR fr) {
        if (!finished) ring.drain(ringHandler);
    }

    private boolean internalTryAndAdd(In in, FR fr) {
        final ILane<In> lane = lanes.lane(Objects.requireNonNull(in));
        final int idx = indexFrom(in);

        if (lane.isFull()) {
            if (!maybeTimeoutHead(idx, lane)) return false;
        }

        final long now = cfg.timeService().currentTimeNanos();
        final String corrId = cfg.correlator().correlationId(in);

        lane.enqueue(in, corrId, now);
        frOffer(idx, fr); // mirror FR for this enqueued item

        if (!running[idx] && permits.tryAcquire()) {
            running[idx] = true;
            submitHead(idx, lane); // uses frPeek(idx) — no capture
        }
        return true;
    }

    private boolean maybeTimeoutHead(int laneIdx, ILane<In> lane) {
        if (timeoutNanos <= 0) return false;

        final long now = cfg.timeService().currentTimeNanos();
        final long headStarted = lane.headStartedAtNanos();
        if ((now - headStarted) < timeoutNanos) return false;

        final In headIn = lane.headT();
        final long elapsed = now - headStarted;

        @SuppressWarnings("unchecked")
        FR headFr = (FR) frPoll(laneIdx); // pop matching FR
        lane.popHead(ignored -> cfg.futureRecord().timedOut(headFr, headIn, elapsed));

        if (!lane.isEmpty()) submitHead(laneIdx, lane);
        else markIdleAndRelease(laneIdx);

        return true;
    }

    private void handleCompletion(FR fr, In in, String corrId, Out out, Throwable error) {
        if (finished) return;

        final ILane<In> lane = lanes.lane(in);
        final int idx = indexFrom(in);

        if (lane.isEmpty() || lane.headT() != in || !lane.headCorrId().equals(corrId)) return; // stale

        // advance lane & FR head in lockstep
        frPoll(idx);
        lane.popHead(ignored -> {
        });

        if (error == null) cfg.futureRecord().completed(fr, out);
        else cfg.futureRecord().failed(fr, in, error);

        if (!lane.isEmpty()) submitHead(idx, lane);
        else markIdleAndRelease(idx);
    }

    private void submitHead(int laneIdx, ILane<In> lane) {
        final In headIn = lane.headT();
        final String corrId = lane.headCorrId();

        @SuppressWarnings("unchecked") final FR headFr = (FR) frPeek(laneIdx); // no capture; pass explicitly
        try {
            userFn.submit(frTypeClass, headFr, headIn, corrId);
        } catch (Throwable submitError) {
            ring.offerFailure(headFr, headIn, corrId, submitError);
        }
    }

    private void markIdleAndRelease(int laneIdx) {
        if (running[laneIdx]) {
            running[laneIdx] = false;
            permits.release();
        }
    }

    private int indexFrom(In in) {
        return cfg.correlator().laneHash(in) & laneMask;
    }

    // ---- FR ring helpers (array-based, zero-GC) ----
    private Object frPeek(int laneIdx) {
        return frCount[laneIdx] == 0 ? null : frRings[laneIdx][frHeadIdx[laneIdx]];
    }

    private void frOffer(int laneIdx, Object fr) {
        if (frCount[laneIdx] == laneDepth) throw new IllegalStateException("FR ring full");
        frRings[laneIdx][frTailIdx[laneIdx]] = fr;
        frTailIdx[laneIdx] = (frTailIdx[laneIdx] + 1) & depthMask;
        frCount[laneIdx]++;
    }

    private Object frPoll(int laneIdx) {
        if (frCount[laneIdx] == 0) return null;
        int h = frHeadIdx[laneIdx];
        Object fr = frRings[laneIdx][h];
        frRings[laneIdx][h] = null;
        frHeadIdx[laneIdx] = (h + 1) & depthMask;
        frCount[laneIdx]--;
        return fr;
    }

    // ---------------------------------------------------------------------
    // Async user function port — FR is explicit (no capture)
    // ---------------------------------------------------------------------
    public interface UserFnPort<I, O, FR> {
        void submit(FutureRecordTypeClass<FR, I, O> tc, FR fr, I in, String corrId);

    }

    @Override
    public ILanes<In> lanes() {
        return lanes;
    }

    @Override
    public Executor executor() {
        return executor;
    }
}
