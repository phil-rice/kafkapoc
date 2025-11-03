package com.hcltech.rmg.common.async;

import com.hcltech.rmg.common.metrics.Metrics;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiConsumer;
import com.hcltech.rmg.common.async.OrderPreservingAsyncExecutor.UserFnPort;
/**
 * Minimal async executor that dispatches work on a pool thread,
 * but posts only a tiny completion record to a local MPSC queue.
 * The operator thread calls drain(...) to run completions & hooks on the operator thread.
 *
 * - No lanes, no ring, no per-key ordering.
 * - No explicit timeouts (kept simple for A/B isolation).
 * - Completions always processed on the operator thread via drain().
 */
public final class SimpleOperatorThreadAsyncExecutor<In, Out, FR>
        implements IOrderPreservingAsyncExecutor<In, Out, FR>,
                   IOrderPreservingAsyncExecutorForTests<In, Out, FR> {

    private final OrderPreservingAsyncExecutorConfig<In, Out, FR> cfg;
    private final FutureRecordTypeClass<FR, In, Out> frTypeClass;
    private final OrderPreservingAsyncExecutor.UserFnPort<In, Out, FR> userFn;
    private final Executor executor;
    private final Metrics metrics;

    private final ConcurrentLinkedQueue<CompletionRecord<In, Out>> q = new ConcurrentLinkedQueue<>();

    private volatile boolean finished = false;

    // operator-thread context (mirrors your existing pattern)
    private FR currentFr;
    private BiConsumer<In, Out> currentHook;

    public SimpleOperatorThreadAsyncExecutor(
            OrderPreservingAsyncExecutorConfig<In, Out, FR> cfg,
            FutureRecordTypeClass<FR, In, Out> frTypeClass,
            UserFnPort<In, Out, FR> userFn,
            Executor executor
    ) {
        this(cfg, frTypeClass, userFn, executor, Metrics.nullMetrics);
    }

    public SimpleOperatorThreadAsyncExecutor(
            OrderPreservingAsyncExecutorConfig<In, Out, FR> cfg,
            FutureRecordTypeClass<FR, In, Out> frTypeClass,
            UserFnPort<In, Out, FR> userFn,
            Executor executor,
            Metrics metrics
    ) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.frTypeClass = Objects.requireNonNull(frTypeClass, "frTypeClass");
        this.userFn = Objects.requireNonNull(userFn, "userFn");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.metrics = metrics == null ? Metrics.nullMetrics : metrics;
    }

    @Override
    public void add(In input, FR fr, BiConsumer<In, Out> hook) {
        if (finished) return;

        // cooperative drain (operator thread)
        drain(fr, hook);

        final String corrId = cfg.correlator().correlationId(input);
        try {
            metrics.increment("async.simple.submit");
            executor.execute(new SubmitTask<>(userFn, frTypeClass, input, corrId, q, metrics));
        } catch (RejectedExecutionException rex) {
            metrics.increment("async.simple.rejected");
            // emulate immediate failure on operator thread path by enqueuing a failure record,
            // then draining in this same call so the hook & FR are invoked on operator thread.
            q.offer(CompletionRecord.failure(input, corrId, rex));
            drain(fr, hook);
        } catch (Throwable t) {
            metrics.increment("async.simple.submit.error");
            q.offer(CompletionRecord.failure(input, corrId, t));
            drain(fr, hook);
        }
    }

    @Override
    public void close() {
        finished = true;
    }

    /**
     * Operator-thread drain: runs completions & hooks on operator thread.
     */
    @Override
    public void drain(FR fr, BiConsumer<In, Out> hook) {
        if (finished) return;
        this.currentFr = fr;
        this.currentHook = hook;
        try {
            metrics.increment("async.simple.drain.calls");
            CompletionRecord<In, Out> rec;
            int n = 0;
            while ((rec = q.poll()) != null) {
                n++;
                if (rec.isSuccess()) {
                    frTypeClass.completed(fr, hook, rec.in(), rec.out());
                    metrics.increment("async.simple.complete.ok");
                } else {
                    frTypeClass.failed(fr, hook, rec.in(), rec.error());
                    metrics.increment("async.simple.complete.fail");
                }
            }
            if (n > 0) metrics.histogram("async.simple.drain.n", n);
        } finally {
            this.currentFr = null;
            this.currentHook = null;
        }
    }

    // ---- Lanes API (not used here) --------------------------------------

    @Override
    public ILanes<In> lanes() {
        return NoopLanesStub.instance();
    }

    // ---------------------------------------------------------------------

    private static final class SubmitTask<I, O, FRX> implements Runnable {
        private final UserFnPort<I, O, FRX> userFn;
        private final FutureRecordTypeClass<FRX, I, O> tc; // not used here, kept for parity
        private final I in;
        private final String corr;
        private final ConcurrentLinkedQueue<CompletionRecord<I, O>> q;
        private final Metrics metrics;

        private final UserFnPort.Completion<I, O> completion = new UserFnPort.Completion<>() {
            @Override
            public void success(I i, String c, O out) {
                metrics.increment("async.simple.enqueue.ok");
                q.offer(CompletionRecord.success(i, c, out));
            }

            @Override
            public void failure(I i, String c, Throwable err) {
                metrics.increment("async.simple.enqueue.fail");
                q.offer(CompletionRecord.failure(i, c, err));
            }
        };

        SubmitTask(UserFnPort<I, O, FRX> userFn,
                   FutureRecordTypeClass<FRX, I, O> tc,
                   I in,
                   String corr,
                   ConcurrentLinkedQueue<CompletionRecord<I, O>> q,
                   Metrics metrics) {
            this.userFn = userFn;
            this.tc = tc;
            this.in = in;
            this.corr = corr;
            this.q = q;
            this.metrics = metrics;
        }

        @Override
        public void run() {
            try {
                userFn.submit(tc, in, corr, completion);
            } catch (Throwable t) {
                metrics.increment("async.simple.submit.throw");
                q.offer(CompletionRecord.failure(in, corr, t));
            }
        }
    }

    // tiny immutable record for completions
    private static final class CompletionRecord<I, O> {
        private final I in;
        private final String corr;
        private final O out;
        private final Throwable error;

        static <I, O> CompletionRecord<I, O> success(I in, String corr, O out) {
            return new CompletionRecord<>(in, corr, out, null);
        }
        static <I, O> CompletionRecord<I, O> failure(I in, String corr, Throwable err) {
            return new CompletionRecord<>(in, corr, null, err);
        }

        private CompletionRecord(I in, String corr, O out, Throwable error) {
            this.in = in; this.corr = corr; this.out = out; this.error = error;
        }
        boolean isSuccess() { return error == null; }
        I in() { return in; }
        String corr() { return corr; }
        O out() { return out; }
        Throwable error() { return error; }
    }

    /**
     * Minimal no-op lanes stub to satisfy the interface.
     */
    private static final class NoopLanesStub<T> implements ILanes<T> {
        private static final NoopLanesStub<?> INSTANCE = new NoopLanesStub<>();
        @SuppressWarnings("unchecked") static <X> NoopLanesStub<X> instance() { return (NoopLanesStub<X>) INSTANCE; }
        @Override public ILane<T> lane(T t) { throw new UnsupportedOperationException("No lanes in SimpleOperatorThreadAsyncExecutor"); }
        @Override public int count() { return 0; }
    }
}
