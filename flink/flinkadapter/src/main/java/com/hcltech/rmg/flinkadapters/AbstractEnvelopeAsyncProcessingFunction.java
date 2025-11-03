package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.common.async.*;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.EnvelopeFailureAdapter;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

/**
 * Custom keyed operator that runs the async executor inside the operator (no AsyncFunction).
 * Completions are drained on the operator thread.
 *
 * K   : String key
 * In  : Envelope<CepState, Msg>
 * Out : Envelope<CepState, Msg>
 */
public abstract class AbstractEnvelopeAsyncProcessingFunction<ESC, CepState, Msg, Schema, FlinkRt, Metrics>
        extends AbstractStreamOperator<Envelope<CepState, Msg>>
        implements OneInputStreamOperator<Envelope<CepState, Msg>, Envelope<CepState, Msg>>,
        ProcessingTimeService.ProcessingTimeCallback,
        KeyContext {

    private static final long DRAIN_INTERVAL_MS = 100L;

    private final AppContainerDefn<ESC, CepState, Msg, Schema, FlinkRt, Output<StreamRecord<Envelope<CepState, Msg>>>, Metrics> appContainerDefn;

    // Toggle: true = use lanes+ring; false = simple operator-thread executor (no lanes/ring)
    private final boolean useLanesAndRing;

    protected transient OrderPreservingAsyncExecutor.UserFnPort<
            Envelope<CepState, Msg>, Envelope<CepState, Msg>, Output<StreamRecord<Envelope<CepState, Msg>>>> userFn;

    protected transient ILanes<Envelope<CepState, Msg>> lanes;
    protected transient IMpscRing<Output<StreamRecord<Envelope<CepState, Msg>>>, Envelope<CepState, Msg>, Envelope<CepState, Msg>> ring;
    protected transient PermitManager permits;
    protected transient ExecutorService ioPool;

    // IMPORTANT: type to the interface so both impls fit
    protected transient IOrderPreservingAsyncExecutorForTests<
            Envelope<CepState, Msg>, Envelope<CepState, Msg>, Output<StreamRecord<Envelope<CepState, Msg>>>> exec;

    protected transient FutureRecordTypeClass<
            Output<StreamRecord<Envelope<CepState, Msg>>>,
            Envelope<CepState, Msg>,
            Envelope<CepState, Msg>> frType;

    public AbstractEnvelopeAsyncProcessingFunction(
            AppContainerDefn<ESC, CepState, Msg, Schema, FlinkRt, Output<StreamRecord<Envelope<CepState, Msg>>>, Metrics> appContainerDefn) {
        this(appContainerDefn, /*useLanesAndRing=*/true);
    }

    public AbstractEnvelopeAsyncProcessingFunction(
            AppContainerDefn<ESC, CepState, Msg, Schema, FlinkRt, Output<StreamRecord<Envelope<CepState, Msg>>>, Metrics> appContainerDefn,
            boolean useLanesAndRing) {
        this.appContainerDefn = appContainerDefn;
        this.useLanesAndRing = useLanesAndRing;
    }

    protected abstract OrderPreservingAsyncExecutor.UserFnPort<Envelope<CepState, Msg>, Envelope<CepState, Msg>, Output<StreamRecord<Envelope<CepState, Msg>>>>
    createUserFnPort(AppContainer<ESC, CepState, Msg, Schema, FlinkRt, Output<StreamRecord<Envelope<CepState, Msg>>>, Metrics> appContainer);

    protected abstract void protectedSetupInOpen(
            AppContainer<ESC, CepState, Msg, Schema, FlinkRt, Output<StreamRecord<Envelope<CepState, Msg>>>, Metrics> appContainer);

    @Override
    public void open() throws Exception {
        super.open();
        int subTask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();

        this.frType = new FlinkOutputFutureRecordAdapter<>(new EnvelopeFailureAdapter<>("someOperation"));

        var container = IAppContainerFactory.resolve(appContainerDefn).valueOrThrow();
        protectedSetupInOpen(container);

        var cfg = container.asyncCfg();
        this.userFn = createUserFnPort(container);

        // Always create the pool (both paths use it)
        ioPool = container.executorServiceFactory().create(Math.max(4, cfg.executorThreads()), "EnvelopeAsyncIOPool-" + subTask);

        if (useLanesAndRing) {
            // original path: lanes + ring + permits + order-preserving executor
            lanes = new Lanes<>(cfg.laneCount(), cfg.laneDepth(), cfg.correlator());
            ring  = new MpscRing<>(Math.max(1024, cfg.maxInFlight() * 2));
            permits = new AtomicPermitManager(cfg.maxInFlight());

            var localCfg = new OrderPreservingAsyncExecutorConfig<>(
                    cfg.laneCount(), cfg.laneDepth(), cfg.maxInFlight(),
                    cfg.executorThreads(), cfg.timeoutMillis(),
                    cfg.correlator(), cfg.failureAdapter(), frType, cfg.timeService()
            );

            exec = new OrderPreservingAsyncExecutor<>(
                    localCfg, lanes, permits, ring, ioPool, frType, cfg.laneCount(), userFn
            );
        } else {
            // simple path: no lanes, no ring — completions run on operator thread via drain()
            exec = new SimpleOperatorThreadAsyncExecutor<>(
                    // reuse same cfg elements the simple exec needs
                    new OrderPreservingAsyncExecutorConfig<>(
                            cfg.laneCount(), cfg.laneDepth(), cfg.maxInFlight(),
                            cfg.executorThreads(), cfg.timeoutMillis(),
                            cfg.correlator(), cfg.failureAdapter(), frType, cfg.timeService()
                    ),
                    frType,
                    (inpFrType, env, corrId, completion) ->
                            userFn.submit(inpFrType, env, corrId, completion),
                    ioPool
            );
            // leave lanes/ring/permits null on this path
            lanes = null;
            ring = null;
            permits = null;
        }

        // schedule periodic drain on operator/mailbox thread
        ProcessingTimeService pts = getProcessingTimeService();
        long now = pts.getCurrentProcessingTime();
        pts.registerTimer(now + DRAIN_INTERVAL_MS, this);
    }

    protected BiConsumer<Envelope<CepState, Msg>, Envelope<CepState, Msg>> createSetKey() {
        return (in, out) -> this.setCurrentKey(in.domainId());
    }

    abstract protected void setKey(Envelope<CepState, Msg> in, Envelope<CepState, Msg> out);

    @Override
    public void processElement(StreamRecord<Envelope<CepState, Msg>> element) throws Exception {
        final Envelope<CepState, Msg> env = element.getValue();
        exec.add(env, output, this::setKey);
        // Optional latency win; timer also drains during idle.
        exec.drain(output, this::setKey);
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        exec.drain(output, this::setKey);
        // re-schedule next tick
        getProcessingTimeService().registerTimer(timestamp + DRAIN_INTERVAL_MS, this);
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
