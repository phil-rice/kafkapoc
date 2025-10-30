package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.common.async.*;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * Keyed, high-throughput async CEP operator using HasSeq type classes.
 * <p>
 * K   = per-key concurrency cap (and ring capacity baseline)
 * M   = subtask-wide concurrency cap (SemaphorePermitManager)
 * In  = input element (sequence handled via HasSeq<In>)
 * Out = output element (sequence handled via HasSeq<Out>)
 * KKey= key type used by keyBy (e.g., domainId)
 */
public class AsyncCepProcessFunction<KKey, In, Out>
        extends KeyedProcessFunction<KKey, In, Out> {

    /**
     * Avoid same-tick re-fire loops; schedule near-immediate ticks.
     */
    private static final long RESCHEDULE_DELTA_MS = 1L;

    // ------------------- config -------------------
    private final int perKeyCapK;
    private final int subtaskCapM;
    private final Executor externalExecutor; // optional injection; null => create internally
    private final Function<In, CompletionStage<Out>> asyncFn;
    private final FailureAdapter<In, Out> failureAdapter;
    private final HasSeq<In> inSeq;
    private final HasSeq<Out> outSeq;

    // ------------------- subtask-scoped -------------------
    private transient ThreadPoolExecutor ownedPool;     // only if externalExecutor == null
    private transient Executor pool;                    // shared for all keys
    private transient SemaphorePermitManager permitsM;  // subtask throttle (M)

    // ------------------- per-key runtime (operator memory) -------------------
    private static final class KeyCtx<EIn, EOut> {
        DefaultPerKeyAsyncExecutor<EIn, EOut> exec;
        boolean timerScheduled;
    }

    private transient Map<KKey, KeyCtx<In, Out>> ctxByKey;

    // ------------------- CEP state (keyed, RocksDB/heap) -------------------
    // Replace Object with your CEP state type
    private transient ValueState<Object> cepState;

    public AsyncCepProcessFunction(
            int perKeyCapK,
            int subtaskCapM,
            Executor externalExecutorOrNull,
            Function<In, CompletionStage<Out>> asyncFn,
            FailureAdapter<In, Out> failureAdapter,
            HasSeq<In> inSeq,
            HasSeq<Out> outSeq) {

        if (perKeyCapK <= 0) throw new IllegalArgumentException("K must be > 0");
        if (subtaskCapM <= 0) throw new IllegalArgumentException("M must be > 0");

        this.perKeyCapK = perKeyCapK;
        this.subtaskCapM = subtaskCapM;
        this.externalExecutor = externalExecutorOrNull;
        this.asyncFn = Objects.requireNonNull(asyncFn, "asyncFn");
        this.failureAdapter = Objects.requireNonNull(failureAdapter, "failureAdapter");
        this.inSeq = Objects.requireNonNull(inSeq, "inSeq");
        this.outSeq = Objects.requireNonNull(outSeq, "outSeq");
    }

    @Override
    public void open(OpenContext parameters) {
        // Defensive: if this instance is being reopened, clean up any previous resources.
        safeCleanup();

        // Build a unique-ish label for resources (thread names etc.)
        final var rt = getRuntimeContext();
        var taskInfo = rt.getTaskInfo();

        final String opLabel = taskInfo.getTaskName() + "#" + taskInfo.getIndexOfThisSubtask() + "@attempt" + taskInfo.getAttemptNumber();

        // Pool
        if (externalExecutor == null) {
            // Cached-style pool: core=0, max=CPU*8 (min 32), threads timeout after 60s, named per opLabel
            int max = Math.max(32, Runtime.getRuntime().availableProcessors() * 8);
            ThreadFactory tf = r -> {
                Thread t = new Thread(r, "async-cep-pool:" + opLabel);
                t.setDaemon(true);
                return t;
            };
            ownedPool = new ThreadPoolExecutor(
                    0,                    // core
                    max,                  // max
                    60L, TimeUnit.SECONDS,
                    new SynchronousQueue<>(),
                    tf
            );
            ownedPool.allowCoreThreadTimeOut(true);
            pool = ownedPool;
        } else {
            pool = externalExecutor; // do not manage lifecycle
        }

        // Subtask-wide throttle (M)
        permitsM = new SemaphorePermitManager(subtaskCapM, false);

        // Per-key registry
        ctxByKey = new HashMap<>();

        // CEP state handle (replace with your descriptor)
        cepState = getRuntimeContext().getState(new ValueStateDescriptor<>("cep", Object.class));
    }

    @Override
    public void close() {
        // Finish and drop per-key executors (idempotent)
        if (ctxByKey != null) {
            for (KeyCtx<In, Out> kctx : ctxByKey.values()) {
                if (kctx.exec != null) {
                    try {
                        kctx.exec.finish();
                    } catch (Throwable ignore) {
                    }
                }
            }
            ctxByKey.clear();
            ctxByKey = null;
        }

        // Shut down owned pool (never touch external one)
        if (ownedPool != null) {
            try {
                ownedPool.shutdownNow();
            } catch (Throwable ignore) {
            }
            ownedPool = null;
        }
        pool = null;

        // Drop references so a subsequent open() cannot see stale instances
        permitsM = null;
        cepState = null;
    }

    @Override
    public void processElement(In in, Context ctx, Collector<Out> out) {
        final KKey key = ctx.getCurrentKey();
        final KeyCtx<In, Out> kctx = ctxByKey.computeIfAbsent(key, k -> newKeyCtx(out));

        // Try to launch; REJECTED_FULL means per-key K or subtask M are full
        AcceptResult ar = kctx.exec.execute(in);

        // Ensure a first wake is scheduled whenever we have in-flight work.
        if (!kctx.timerScheduled && ar != AcceptResult.REJECTED_FULL) {
            long now = ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(now + RESCHEDULE_DELTA_MS);
            kctx.timerScheduled = true;
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Out> out) {
        final KKey key = ctx.getCurrentKey();
        final KeyCtx<In, Out> kctx = ctxByKey.get(key);
        if (kctx == null) return;

        // allow re-scheduling inside onWake()
        kctx.timerScheduled = false;

        // Drain to quiescence in this tick to avoid multi-tick flakiness.
        // Keep a sane cap to prevent pathological tight loops.
        int spins = 0;
        do {
            kctx.exec.onWake();
            spins++;
        } while (kctx.exec.needsWake() && spins < 1024);

        // If work still remains (extreme case), schedule a near-immediate tick.
        if (kctx.exec.needsWake()) {
            long now = ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(now + RESCHEDULE_DELTA_MS);
            kctx.timerScheduled = true;
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    /**
     * Best-effort cleanup for re-entrant open() or normal close().
     */
    void safeCleanup() {
        // Finish existing per-key executors
        if (ctxByKey != null) {
            for (KeyCtx<In, Out> kctx : ctxByKey.values()) {
                if (kctx.exec != null) {
                    try {
                        kctx.exec.finish();
                    } catch (Throwable ignore) {
                    }
                }
            }
            ctxByKey.clear();
            ctxByKey = null;
        }
        // Shut down previously owned pool, if any
        if (ownedPool != null) {
            try {
                ownedPool.shutdownNow();
            } catch (Throwable ignore) {
            }
            ownedPool = null;
        }
        pool = null;
        permitsM = null;
        // cepState is re-acquired via RuntimeContext below; just null it here
        cepState = null;
    }

    /**
     * Build a per-key executor that applies CEP mutations + emits inside handleOrdered(out).
     */
    private KeyCtx<In, Out> newKeyCtx(Collector<Out> collector) {
        KeyCtx<In, Out> kc = new KeyCtx<>();
        kc.exec = new DefaultPerKeyAsyncExecutor<>(
                perKeyCapK,
                pool,
                asyncFn,
                failureAdapter,
                permitsM,
                inSeq,
                outSeq
        ) {
            @Override
            protected void handleOrdered(Out out) {
                // 1) Read/modify CEP state (keyed, only on operator thread)
                // Object s = cepState.value();  // replace with your CEP type
                // ... apply out’s mutations to s ...
                // cepState.update(s);
                // 2) Emit downstream (in-order)
                collector.collect(out);
            }
        };
        kc.timerScheduled = false;
        return kc;
    }

    ThreadPoolExecutor getOwnedPoolForTest() {
        return ownedPool;
    }

    Map<KKey, KeyCtx<In, Out>> getCtxByKeyForTest() {
        return ctxByKey;
    }
}
