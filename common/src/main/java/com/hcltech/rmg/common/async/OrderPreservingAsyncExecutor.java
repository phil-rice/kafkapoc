package com.hcltech.rmg.common.async;

import java.util.Objects;
import java.util.concurrent.Executor;
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

    private final boolean[] running;
    private final long timeoutNanos;
    private volatile boolean finished = false;

    private final int laneCount, laneMask;

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
    }

    // ===================================================================================

    @Override
    public void add(In input, FR fr, BiConsumer<In, Out> hook) {
        while (!finished) {
            // Cooperative drain with the CURRENT fr/hook (operator thread)
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
     */
    public void drain(final FR fr, final BiConsumer<In, Out> hook) {
        if (finished) return;

        IMpscRing.Handler<FR, In, Out> handler = new IMpscRing.Handler<>() {
            @Override
            public void onSuccess(FR _ignored, BiConsumer<In, Out> _ignored2,
                                  In in, String corrId, Out out) {
                handleCompletion(fr, hook, in, corrId, out, null);
            }

            @Override
            public void onFailure(FR _ignored, BiConsumer<In, Out> _ignored2,
                                  In in, String corrId, Throwable error) {
                handleCompletion(fr, hook, in, corrId, null, error);
            }
        };

        // We do not rely on the ring's onCompleteOrFailed; pass a no-op.
        ring.drain((a, b) -> {
        }, handler);
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
            submitHead(idx, lane); // UserFn submit has no FR/hook; commit will use CURRENT fr/hook in drain
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

        // stale or re-ordered (shouldn't happen with ordered exec)
        if (lane.isEmpty() || lane.headT() != in || !lane.headCorrId().equals(corrId)) return;

        // Pop head
        lane.popHead(ignored -> {
        });

        if (error == null) cfg.futureRecord().completed(fr, hook, in, out);
        else cfg.futureRecord().failed(fr, hook, in, error);

        if (!lane.isEmpty()) submitHead(idx, lane);
        else markIdleAndRelease(idx);
    }

    private UserFnPort.Completion<In, Out> submitCompletion = new UserFnPort.Completion<>() {
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
        return cfg.correlator().laneHash(in) & laneMask;
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
    public ILanes<In> lanes() {
        return lanes;
    }

    @Override
    public Executor executor() {
        return executor;
    }
}
