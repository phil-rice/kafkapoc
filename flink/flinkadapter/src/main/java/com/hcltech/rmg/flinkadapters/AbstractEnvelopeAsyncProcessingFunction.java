package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.common.async.*;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.EnvelopeFailureAdapter;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Keyed operator that keeps CEP state and runs the OrderPreservingAsyncExecutor
 * in the same operator (no AsyncFunction). Completions are drained on the operator
 * thread and emitted directly to the Collector (no retention).
 * <p>
 * K   : Flink key type (String)
 * In  : Envelope<CepState, Msg>
 * Out : Envelope<CepState, Msg>
 */
public abstract class AbstractEnvelopeAsyncProcessingFunction<ESC, CepState, Msg, Schema, FlinkRt, Metrics>
        extends KeyedProcessFunction<String, Envelope<CepState, Msg>, Envelope<CepState, Msg>> {

    private final AppContainerDefn<ESC, CepState, Msg, Schema, FlinkRt, Collector<Envelope<CepState, Msg>>, Metrics> appContainerDefn;

    // NOTE: UserFnPort now carries FR explicitly.
    private transient OrderPreservingAsyncExecutor.UserFnPort<Envelope<CepState, Msg>, Envelope<CepState, Msg>, Collector<Envelope<CepState, Msg>>> userFn;

    private transient ILanes<Envelope<CepState, Msg>> lanes;
    private transient IMpscRing<Collector<Envelope<CepState, Msg>>, Envelope<CepState, Msg>, Envelope<CepState, Msg>> ring;
    private transient PermitManager permits;
    private transient ExecutorService ioPool;
    private transient OrderPreservingAsyncExecutor<
            Envelope<CepState, Msg>, Envelope<CepState, Msg>, Collector<Envelope<CepState, Msg>>> exec;

    // FR typeclass emits directly to the provided Collector (FR)
    private transient FutureRecordTypeClass<Collector<Envelope<CepState, Msg>>, Envelope<CepState, Msg>, Envelope<CepState, Msg>> frType;

    public AbstractEnvelopeAsyncProcessingFunction(AppContainerDefn<ESC, CepState, Msg, Schema, FlinkRt, Collector<Envelope<CepState, Msg>>, Metrics> appContainerDefn) {
        this.appContainerDefn = appContainerDefn;
    }

    abstract protected OrderPreservingAsyncExecutor.UserFnPort<Envelope<CepState, Msg>, Envelope<CepState, Msg>, Collector<Envelope<CepState, Msg>>> createUserFnPort(
            AppContainer<ESC, CepState, Msg, Schema, FlinkRt, Collector<Envelope<CepState, Msg>>, Metrics> appContainer);

    abstract void protectedSetupInOpen(AppContainer<ESC, CepState, Msg, Schema, FlinkRt, Collector<Envelope<CepState, Msg>>, Metrics> appContainer);

    @Override
    public void open(OpenContext parameters) {
        this.frType = new FlinkCollectorFutureRecordAdapter<>(new EnvelopeFailureAdapter<>("someOperation"));
        var container = IAppContainerFactory.resolve(appContainerDefn).valueOrThrow();
        protectedSetupInOpen(container);
        var cfg = container.asyncCfg();
        this.userFn = createUserFnPort(container);
        lanes = new Lanes<>(cfg.laneCount(), cfg.laneDepth(), cfg.correlator());
        ring = new MpscRing<>(Math.max(1024, cfg.maxInFlight() * 2));
        permits = new AtomicPermitManager(cfg.maxInFlight());
        ioPool = Executors.newFixedThreadPool(Math.max(4, cfg.executorThreads()));

        // Use the FR typeclass that knows how to emit to the Collector (FR)
        var localCfg = new OrderPreservingAsyncExecutorConfig<>(
                cfg.laneCount(), cfg.laneDepth(), cfg.maxInFlight(),
                cfg.executorThreads(), cfg.timeoutMillis(),
                cfg.correlator(), cfg.failureAdapter(), frType, cfg.timeService()
        );

        exec = new OrderPreservingAsyncExecutor<>(
                localCfg, lanes, permits, ring, ioPool, frType, cfg.laneCount(), userFn
        );
    }

    @Override
    public void processElement(Envelope<CepState, Msg> value,
                               Context ctx,
                               Collector<Envelope<CepState, Msg>> out) throws Exception {
        // Pass the Collector as FR for this item; executor will cooperatively drain.
        exec.add(value, out);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Envelope<CepState, Msg>> out) throws Exception {
        // Periodic drain to flush completions; FR is carried per event in the ring.
        exec.drain(null);
    }

    @Override
    public void close() {
        try {
            if (exec != null) exec.close();
        } finally {
            if (ioPool != null) ioPool.shutdownNow();
        }
    }
}
