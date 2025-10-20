package com.hcltech.rmg.common.async;

import com.hcltech.rmg.common.function.TriConsumer;

import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * Order-preserving async executor (single operator thread API).
 * Zero-GC hot path: per-lane FR storage, reusable ring handler, reusable completion,
 * and preconstructed success/failure handlers.
 */
public final class OrderPreservingAsyncExecutor<In, Out, FR>
        implements IOrderPreservingAsyncExecutor<In, Out, FR>,
        IOrderPreservingAsyncExecutorForTests<In, Out, FR> {

    private final OrderPreservingAsyncExecutorConfig<In, Out, FR> cfg;
    private final ILanes<FR, In> lanes;
    private final PermitManager permits;
    private final IMpscRing<In, Out> ring;
    private final Executor executor;
    private final UserFnPort<In, Out> userFn;

    private final boolean[] running; // one flag per lane
    private final CircularBufferWithCallback<Out> outBuffer;
    private long emitSeq = 0L;
    private final long timeoutNanos;
    private volatile boolean finished = false;

    // single reusable ring handler
    private final IMpscRing.Handler<In, Out> ringHandler = new IMpscRing.Handler<>() {
        @Override
        public void onSuccess(In in, long corrId, Out out) {
            handleCompletion(in, corrId, out, null);
        }

        @Override
        public void onFailure(In in, long corrId, Throwable error) {
            handleCompletion(in, corrId, null, error);
        }
    };

    // reusable completion adapter
    private final UserFnPort.Completion<In, Out> completion = new UserFnPort.Completion<>() {
        @Override
        public void success(In in, long corrId, Out out) {
            ring.offerSuccess(in, corrId, out);
        }

        @Override
        public void failure(In in, long corrId, Throwable err) {
            ring.offerFailure(in, corrId, err);
        }
    };

    // prebuilt lane pop handlers (initialised in constructor)
    private final TriConsumer<FR, In, Out> onSuccess;
    private final TriConsumer<FR, In, Throwable> onFailure;

    public OrderPreservingAsyncExecutor(
            OrderPreservingAsyncExecutorConfig<In, Out, FR> cfg,
            ILanes<FR, In> lanes,
            PermitManager permits,
            IMpscRing<In, Out> ring,
            Executor executor,
            int laneCount,
            UserFnPort<In, Out> userFn
    ) {
        this.cfg = Objects.requireNonNull(cfg);
        this.lanes = Objects.requireNonNull(lanes);
        this.permits = Objects.requireNonNull(permits);
        this.ring = Objects.requireNonNull(ring);
        this.executor = Objects.requireNonNull(executor);
        this.userFn = Objects.requireNonNull(userFn);
        if (laneCount <= 0) throw new IllegalArgumentException("laneCount must be > 0");
        this.running = new boolean[laneCount];
        this.timeoutNanos = cfg.timeoutMillis() <= 0 ? 0L : cfg.timeoutMillis() * 1_000_000L;

        this.outBuffer = new CircularBufferWithCallback<>(
                Math.min(Math.max(cfg.maxInFlight(), 8), 1 << 16),
                o -> {
                },
                (v, t) -> {
                }
        );

        // now we can safely capture cfg / outBuffer
        this.onSuccess = (fr, in, out) -> {
            if (fr != null) {
                cfg.futureRecord().completed(fr, out);
                outBuffer.put(emitSeq++, out);
            }
        };
        this.onFailure = (fr, in, err) -> {
            if (fr != null) cfg.futureRecord().failed(fr, in, err);
        };
    }

    // =====================================================================
    // public API
    // =====================================================================

    @Override
    public void add(In input, FR futureRecord) {
        while (!finished) {
            drain();
            if (internalTryAndAdd(input, futureRecord)) return;
        }
    }

    @Override
    public void close() {
        finished = true;
    }

    @Override
    public void drain() {
        if (!finished) ring.drain(ringHandler);
    }

    @Override
    public ILanes<FR, In> lanes() {
        return lanes;
    }

    @Override
    public Executor executor() {
        return executor;
    }

    @Override
    public CircularBufferWithCallback<Out> outBuffer() {
        return outBuffer;
    }

    // =====================================================================
    // core logic
    // =====================================================================

    private boolean internalTryAndAdd(In in, FR fr) {
        final ILane<FR, In> lane = lanes.lane(Objects.requireNonNull(in));
        if (lane.isFull()) {
            if (!maybeTimeoutHead(lane, indexFrom(in))) return false;
        }

        final long now = cfg.timeService().currentTimeNanos();
        final long corrId = cfg.correlator().correlationId(in);
        lane.enqueue(in, fr, corrId, now);

        final int idx = indexFrom(in);
        if (!running[idx] && permits.tryAcquire()) {
            running[idx] = true;
            submitHead(lane);
        }
        return true;
    }

    private boolean maybeTimeoutHead(ILane<FR, In> lane, int laneIdx) {
        if (timeoutNanos <= 0) return false;
        final long now = cfg.timeService().currentTimeNanos();
        if ((now - lane.headStartedAtNanos()) < timeoutNanos) return false;

        final In headIn = lane.headT();
        final long elapsed = now - lane.headStartedAtNanos();

        lane.popHead((frHead, ignoredT) -> {
            if (frHead != null) {
                cfg.futureRecord().timedOut(frHead, headIn, elapsed);
                Out mapped = cfg.failureAdapter().onTimeout(headIn, elapsed);
                outBuffer.put(emitSeq++, mapped);
            }
        });

        if (!lane.isEmpty()) submitHead(lane);
        else markIdleAndRelease(laneIdx);
        return true;
    }

    private void handleCompletion(In in, long corrId, Out out, Throwable error) {
        if (finished) return;
        final ILane<FR, In> lane = lanes.lane(in);
        if (lane.isEmpty() || lane.headT() != in || lane.headCorrId() != corrId) return; // stale

        if (error == null) lane.popHead(out, onSuccess);
        else lane.popHead(error, onFailure);

        if (!lane.isEmpty()) submitHead(lane);
        else markIdleAndRelease(indexFrom(in));
    }

    private void submitHead(ILane<FR, In> lane) {
        final In headIn = lane.headT();
        final long headCorr = lane.headCorrId();
        try {
            userFn.submit(headIn, headCorr, completion);
        } catch (Throwable submitError) {
            ring.offerFailure(headIn, headCorr, submitError);
        }
    }

    private void markIdleAndRelease(int laneIdx) {
        if (running[laneIdx]) {
            running[laneIdx] = false;
            permits.release();
        }
    }

    private int indexFrom(In in) {
        return cfg.correlator().laneHash(in) & (running.length - 1);
    }

    // ---------------------------------------------------------------------
    // Async user function port
    // ---------------------------------------------------------------------
    public interface UserFnPort<I, O> {
        void submit(I in, long corrId, Completion<I, O> completion);

        interface Completion<I, O> {
            void success(I in, long corrId, O out);

            void failure(I in, long corrId, Throwable err);
        }
    }
}
