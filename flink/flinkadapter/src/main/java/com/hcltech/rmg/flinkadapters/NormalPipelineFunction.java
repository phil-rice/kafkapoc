package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.all_execution.BizLogicPipelineStep;
import com.hcltech.rmg.all_execution.EnrichmentPipelineStep;
import com.hcltech.rmg.all_execution.ParseMessagePipelineStep;
import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.common.ITimeService;
import com.hcltech.rmg.flink_metrics.FlinkMetricsParams;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.metrics.EnvelopeMetrics;
import com.hcltech.rmg.metrics.EnvelopeMetricsTC;
import com.hcltech.rmg.metrics.Metrics;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


public class NormalPipelineFunction<MSC, CepState, Msg, RT, Schema> extends RichAsyncFunction<Envelope<CepState, Msg>, Envelope<CepState, Msg>> {
    private final AppContainerDefn<MSC, CepState, Msg, Schema, RT, FlinkMetricsParams> appContainerDefn;
    private final String module;
    transient private EnrichmentPipelineStep<MSC, CepState, Msg, Schema, RT, FlinkMetricsParams> enrichmentPipelineStep;
    transient private BizLogicPipelineStep<MSC, CepState, Msg, Schema, RT, FlinkMetricsParams> bizLogic;
    transient private ParseMessagePipelineStep<MSC, CepState, Msg, Schema, RT, FlinkMetricsParams> parser;
    transient private EnvelopeMetrics<Envelope<?, ?>> envelopeMetrics;
    transient private Metrics metrics;
    transient private ITimeService timeService;

    public NormalPipelineFunction(AppContainerDefn<MSC, CepState, Msg, Schema, RT, FlinkMetricsParams> appContainerDefn, String module) {
        this.appContainerDefn = appContainerDefn;
        this.module = module;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        var container = IAppContainerFactory.resolve(appContainerDefn).valueOrThrow();
        this.parser = new ParseMessagePipelineStep<>(container);
        this.enrichmentPipelineStep = new EnrichmentPipelineStep<>(container, module);
        this.bizLogic = new BizLogicPipelineStep<>(container, null, module);
        var metricsFactory = container.metricsFactory();
        var params = FlinkMetricsParams.fromRuntime(getRuntimeContext(), getClass());
        this.metrics = metricsFactory.create(params);
        this.envelopeMetrics = EnvelopeMetrics.create(container.timeService(), metrics, EnvelopeMetricsTC.INSTANCE);
        this.timeService = container.timeService();
    }

    @Override
    public void asyncInvoke(Envelope<CepState, Msg> cepStateMsgEnvelope, ResultFuture<Envelope<CepState, Msg>> resultFuture) throws Exception {
        long start = timeService.currentTimeNanos();
        var afterParse = parser.parse(cepStateMsgEnvelope);
        var afterEnrichment = enrichmentPipelineStep.process(afterParse);
        var afterBizLogic = bizLogic.process(afterEnrichment);
        envelopeMetrics.addToMetricsAtEnd(afterBizLogic);
        long finish = timeService.currentTimeNanos();
        long duration = finish - start;
        metrics.histogram("NormalPipelineFunction.asyncInvoke.millis", duration);
        afterBizLogic.valueEnvelope().setDurationNanos(duration);
        resultFuture.complete(List.of(afterBizLogic));
    }
}

