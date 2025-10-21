package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.common.async.*;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.EnvelopeFailureAdapter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

/**
 * Custom keyed operator that runs the OrderPreservingAsyncExecutor inside the operator
 * (no AsyncFunction). Completions are drained on the operator thread.
 * <p>
 * K   : String key
 * In  : Envelope<CepState, Msg>
 * Out : Envelope<CepState, Msg>
 */
public abstract class AbstractEnvelopeAsyncProcessingFunction<ESC, CepState, Msg, Schema, FlinkRt, Metrics>
        extends AbstractStreamOperator<Envelope<CepState, Msg>>
        implements OneInputStreamOperator<Envelope<CepState, Msg>, Envelope<CepState, Msg>>,
        KeyContext {

    private final AppContainerDefn<ESC, CepState, Msg, Schema, FlinkRt, Output<StreamRecord<Envelope<CepState, Msg>>>, Metrics> appContainerDefn;

    protected transient OrderPreservingAsyncExecutor.UserFnPort<
            Envelope<CepState, Msg>, Envelope<CepState, Msg>, Output<StreamRecord<Envelope<CepState, Msg>>>> userFn;

    protected transient ILanes<Envelope<CepState, Msg>> lanes;
    protected transient IMpscRing<Output<StreamRecord<Envelope<CepState, Msg>>>, Envelope<CepState, Msg>, Envelope<CepState, Msg>> ring;
    protected transient PermitManager permits;
    protected transient ExecutorService ioPool;
    protected transient OrderPreservingAsyncExecutor<
            Envelope<CepState, Msg>, Envelope<CepState, Msg>, Output<StreamRecord<Envelope<CepState, Msg>>>> exec;

    protected transient FutureRecordTypeClass<
            Output<StreamRecord<Envelope<CepState, Msg>>>,
            Envelope<CepState, Msg>,
            Envelope<CepState, Msg>> frType;

    public AbstractEnvelopeAsyncProcessingFunction(
            AppContainerDefn<ESC, CepState, Msg, Schema, FlinkRt, Output<StreamRecord<Envelope<CepState, Msg>>>, Metrics> appContainerDefn) {
        this.appContainerDefn = appContainerDefn;
    }

    protected abstract OrderPreservingAsyncExecutor.UserFnPort<Envelope<CepState, Msg>, Envelope<CepState, Msg>, Output<StreamRecord<Envelope<CepState, Msg>>>>
    createUserFnPort(AppContainer<ESC, CepState, Msg, Schema, FlinkRt, Output<StreamRecord<Envelope<CepState, Msg>>>, Metrics> appContainer);

    protected abstract void protectedSetupInOpen(
            AppContainer<ESC, CepState, Msg, Schema, FlinkRt, Output<StreamRecord<Envelope<CepState, Msg>>>, Metrics> appContainer);

    @Override
    public void open() throws Exception { // <-- operator-style open (no OpenContext)
        super.open();

        // typeclass that emits to this operator's output (FR = Collector)
        this.frType = new FlinkOutputFutureRecordAdapter<>(new EnvelopeFailureAdapter<>("someOperation"));

        var container = IAppContainerFactory.resolve(appContainerDefn).valueOrThrow();
        protectedSetupInOpen(container);

        var cfg = container.asyncCfg();
        this.userFn = createUserFnPort(container);

        lanes = new Lanes<>(cfg.laneCount(), cfg.laneDepth(), cfg.correlator());
        ring = new MpscRing<>(Math.max(1024, cfg.maxInFlight() * 2));
        permits = new AtomicPermitManager(cfg.maxInFlight());
        ioPool = Executors.newFixedThreadPool(Math.max(4, cfg.executorThreads()));


        // Build executor config
        var localCfg = new OrderPreservingAsyncExecutorConfig<>(
                cfg.laneCount(), cfg.laneDepth(), cfg.maxInFlight(),
                cfg.executorThreads(), cfg.timeoutMillis(),
                cfg.correlator(), cfg.failureAdapter(), frType, cfg.timeService()
        );

        // Executor (pass setKeyFor if your ctor accepts it; else thread it via config)
        exec = new OrderPreservingAsyncExecutor<>(
                localCfg, lanes, permits, ring, ioPool, frType, cfg.laneCount(), userFn /*, setKeyFor */
        );
    }

    protected BiConsumer<Envelope<CepState, Msg>, Envelope<CepState, Msg>> createSetKey() {
        BiConsumer<Envelope<CepState, Msg>, Envelope<CepState, Msg>> setKeyFor = (in, out) -> {
            String key = in.domainId();
            this.setCurrentKey(key);
        };
        return setKeyFor;
    }

    abstract protected void setKey(Envelope<CepState, Msg> in, Envelope<CepState, Msg> out);

    @Override
    public void processElement(StreamRecord<Envelope<CepState, Msg>> element) throws Exception {
        final Envelope<CepState, Msg> env = element.getValue();

        exec.add(env, output, this::setKey);
    }

    @Override
    public void close() throws Exception {
        try {
            if (exec != null) exec.close();
        } finally {
            if (ioPool != null) ioPool.shutdownNow();
            super.close();
        }
    }
}
