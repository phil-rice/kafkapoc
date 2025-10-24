package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.all_execution.BizLogicPipelineStep;
import com.hcltech.rmg.all_execution.EnrichmentPipelineStep;
import com.hcltech.rmg.all_execution.ParseMessagePipelineStep;
import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.common.ITimeService;
import com.hcltech.rmg.common.function.Callback;
import com.hcltech.rmg.flink_metrics.FlinkMetricsParams;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.ValueEnvelope;
import com.hcltech.rmg.metrics.EnvelopeMetrics;
import com.hcltech.rmg.metrics.EnvelopeMetricsTC;
import com.hcltech.rmg.metrics.Metrics;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;

public final class NormalPipelineFunction<MSC, CepState, Msg, RT, FR, Schema>
        extends RichAsyncFunction<Envelope<CepState, Msg>, Envelope<CepState, Msg>> {

    private final AppContainerDefn<MSC, CepState, Msg, Schema, RT, FR, FlinkMetricsParams> appContainerDefn;
    private final String module;

    // Initialized in open() or via initForTest()
    private transient ParseMessagePipelineStep<MSC, CepState, Msg, Schema, RT, FR, FlinkMetricsParams> parser;
    private transient EnrichmentPipelineStep<MSC, CepState, Msg, Schema, RT, FR, FlinkMetricsParams>  enrichment;
    private transient BizLogicPipelineStep<MSC, CepState, Msg, Schema, RT, FR, FlinkMetricsParams>    bizLogic;
    private transient EnvelopeMetrics<Envelope<?, ?>>                                                  envelopeMetrics;
    private transient Metrics                                                                          metrics;
    private transient ITimeService                                                                     time;

    public NormalPipelineFunction(
            AppContainerDefn<MSC, CepState, Msg, Schema, RT, FR, FlinkMetricsParams> appContainerDefn,
            String module) {
        this.appContainerDefn = appContainerDefn;
        this.module = module;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        var container = IAppContainerFactory.resolve(appContainerDefn).valueOrThrow();

        this.parser     = new ParseMessagePipelineStep<>(container);           // sync
        this.enrichment = new EnrichmentPipelineStep<>(container, module);     // async-shaped
        this.bizLogic   = new BizLogicPipelineStep<>(container, null, module); // async-shaped

        var params = FlinkMetricsParams.fromRuntime(getRuntimeContext(), getClass());
        this.metrics         = container.metricsFactory().create(params);
        this.envelopeMetrics = EnvelopeMetrics.create(container.timeService(), metrics, EnvelopeMetricsTC.INSTANCE);
        this.time            = container.timeService();
    }

    /**
     * Package-private helper to inject test doubles without reflection.
     * Intended for tests only.
     */
    void initForTest(
            ParseMessagePipelineStep<MSC, CepState, Msg, Schema, RT, FR, FlinkMetricsParams> parser,
            EnrichmentPipelineStep<MSC, CepState, Msg, Schema, RT, FR, FlinkMetricsParams> enrichment,
            BizLogicPipelineStep<MSC, CepState, Msg, Schema, RT, FR, FlinkMetricsParams> bizLogic,
            EnvelopeMetrics<Envelope<?, ?>> envelopeMetrics,
            Metrics metrics,
            ITimeService time
    ) {
        this.parser = parser;
        this.enrichment = enrichment;
        this.bizLogic = bizLogic;
        this.envelopeMetrics = envelopeMetrics;
        this.metrics = metrics;
        this.time = time;
    }

    @Override
    public void asyncInvoke(Envelope<CepState, Msg> in,
                            ResultFuture<Envelope<CepState, Msg>> out) {

        final long t0 = time.currentTimeNanos();

        // 1) Parse synchronously (now guarded so failures complete the future)
        final Envelope<CepState, Msg> afterParse;
        try {
            afterParse = parser.parse(in);
        } catch (Throwable t) {
            out.completeExceptionally(t);
            return;
        }

        // 2) Kick off the async chain using a reusable, per-thread runner as the only callback
        final StepRunner<CepState, Msg> runner = StepRunner.acquire();
        runner.init(enrichment, bizLogic, envelopeMetrics, metrics, time, out, t0);
        runner.start(afterParse);
    }

    /**
     * Reusable continuation that drives enrichment → biz → emit.
     * Implements Callback so we can pass it directly to both steps.
     */
    private static final class StepRunner<CS, M> implements Callback<Envelope<CS, M>> {

        // Per-thread pool (avoid per-call allocation of the runner)
        private static final ThreadLocal<Deque<StepRunner<?, ?>>> POOL =
                ThreadLocal.withInitial(ArrayDeque::new);

        @SuppressWarnings("unchecked")
        static <CS, M> StepRunner<CS, M> acquire() {
            Deque<StepRunner<?, ?>> q = POOL.get();
            return q.isEmpty() ? new StepRunner<>() : (StepRunner<CS, M>) q.pop();
        }

        private static void release(StepRunner<?, ?> r) {
            r.clear();
            POOL.get().push(r);
        }

        // Wiring / context
        private EnrichmentPipelineStep<?, CS, M, ?, ?, ?, ?> enrichment;
        private BizLogicPipelineStep<?, CS, M, ?, ?, ?, ?>    bizLogic;
        private EnvelopeMetrics<Envelope<?, ?>>               envelopeMetrics;
        private Metrics                                       metrics;
        private ITimeService                                  time;
        private ResultFuture<Envelope<CS, M>>                 out;

        // Ephemeral state
        private long    startNanos;
        private boolean afterEnrichment;

        void init(EnrichmentPipelineStep<?, CS, M, ?, ?, ?, ?> enrichment,
                  BizLogicPipelineStep<?, CS, M, ?, ?, ?, ?>    bizLogic,
                  EnvelopeMetrics<Envelope<?, ?>>               envelopeMetrics,
                  Metrics                                       metrics,
                  ITimeService                                  time,
                  ResultFuture<Envelope<CS, M>>                 out,
                  long startNanos) {
            this.enrichment      = enrichment;
            this.bizLogic        = bizLogic;
            this.envelopeMetrics = envelopeMetrics;
            this.metrics         = metrics;
            this.time            = time;
            this.out             = out;
            this.startNanos      = startNanos;
            this.afterEnrichment = false;
        }

        /** Step 2: start enrichment (or skip to biz if not a ValueEnvelope). */
        void start(Envelope<CS, M> afterParse) {
            try {
                if (afterParse instanceof ValueEnvelope<?, ?>) {
                    enrichment.call(afterParse, this); // may callback inline → success() will dispatch to biz
                } else {
                    // Not a ValueEnvelope → no enrichment/bizlogic; pass straight through.
                    onBizDone(afterParse);
                }
            } catch (Throwable t) {
                fail(t);
            }
        }

        // Callback implementation: both stages call back here
        @Override
        public void success(Envelope<CS, M> value) {
            try {
                if (!afterEnrichment) {
                    // 3a) got enriched value → move to biz logic
                    afterEnrichment = true;
                    bizLogic.call(value, this);        // may callback inline → success() falls through next time
                    return;
                }
                // 3b) biz logic done → finish
                onBizDone(value);
            } catch (Throwable t) {
                fail(t);
            }
        }

        @Override
        public void failure(Throwable error) {
            fail(error);
        }

        /** Step 4: finalize metrics and emit the single output. */
        private void onBizDone(Envelope<CS, M> resultEnvelope) {
            try {
                envelopeMetrics.addToMetricsAtEnd(resultEnvelope);

                final long durNanos = time.currentTimeNanos() - startNanos;
                // BUGFIX: record nanos, not "millis"
                metrics.histogram("NormalPipelineFunction.asyncInvoke.nanos", durNanos);

                if (resultEnvelope instanceof ValueEnvelope<?, ?>) {
                    @SuppressWarnings("unchecked")
                    ValueEnvelope<CS, M> ve = (ValueEnvelope<CS, M>) resultEnvelope;
                    ve.setDurationNanos(durNanos);
                }

                out.complete(Collections.singletonList(resultEnvelope));
            } catch (Throwable t) {
                out.completeExceptionally(t);
            } finally {
                release(this);
            }
        }

        private void fail(Throwable t) {
            try {
                out.completeExceptionally(t);
            } finally {
                release(this);
            }
        }

        private void clear() {
            enrichment = null;
            bizLogic = null;
            envelopeMetrics = null;
            metrics = null;
            time = null;
            out = null;
            startNanos = 0L;
            afterEnrichment = false;
        }
    }
}
