package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.all_execution.BizLogicPipelineStep;
import com.hcltech.rmg.all_execution.EnrichmentPipelineStep;
import com.hcltech.rmg.all_execution.ParseMessagePipelineStep;
import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.cepstate.CepEventLog;
import com.hcltech.rmg.cepstate.CepStateTypeClass;
import com.hcltech.rmg.common.ITimeService;
import com.hcltech.rmg.common.async.OrderPreservingAsyncExecutor;
import com.hcltech.rmg.flink_metrics.FlinkMetricsParams;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.ValueEnvelope;
import com.hcltech.rmg.metrics.EnvelopeMetrics;
import com.hcltech.rmg.metrics.EnvelopeMetricsTC;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * Keyed operator that keeps CEP state and runs the OrderPreservingAsyncExecutor
 * in the same operator (no AsyncFunction). Completions are drained on the operator
 * thread and emitted directly to the Collector (no retention).
 * <p>
 * K   : Flink key type (String)
 * In  : Envelope<CepState, Msg>
 * Out : Envelope<CepState, Msg>
 */
public class EnvelopeAIAsyncProcessingFunction<ESC, CepState, Msg, Schema>
        extends AbstractEnvelopeAsyncProcessingFunction<ESC, CepState, Msg, Schema, RuntimeContext, FlinkMetricsParams> {

    private final String module;
    private ParseMessagePipelineStep<ESC, CepState, Msg, Schema, RuntimeContext, Output<StreamRecord<Envelope<CepState, Msg>>>, FlinkMetricsParams> parser;
    private EnrichmentPipelineStep<ESC, CepState, Msg, Schema, RuntimeContext, Output<StreamRecord<Envelope<CepState, Msg>>>, FlinkMetricsParams> enrichmentPipelineStep;
    private BizLogicPipelineStep<ESC, CepState, Msg, Schema, RuntimeContext, Output<StreamRecord<Envelope<CepState, Msg>>>, FlinkMetricsParams> bizLogic;
    private com.hcltech.rmg.metrics.Metrics metrics;
    private EnvelopeMetrics<Envelope<?, ?>> envelopeMetrics;
    private ITimeService timeService;
    private CepStateTypeClass<CepState> cepStateTypeClass;
    private CepEventLog cepEventLog;
    private BiConsumer<Envelope<CepState, Msg>, Envelope<CepState, Msg>> setKey;

    public EnvelopeAIAsyncProcessingFunction(AppContainerDefn appContainerDefn, String module) {
        super(appContainerDefn);
        this.module = module;
    }

    @Override
    public void processElement(StreamRecord<Envelope<CepState, Msg>> record) throws Exception {
        var env = record.getValue();
        if (env instanceof ValueEnvelope<CepState, Msg> ve) {
            var cepState = cepEventLog.foldAll(cepStateTypeClass, cepStateTypeClass.createEmpty());
            ve.setCepState(cepState);
        }
        super.processElement(record);
    }


    @Override
    protected void setKey(Envelope<CepState, Msg> in, Envelope<CepState, Msg> out) {
        if (out instanceof ValueEnvelope<CepState, Msg> veOut) {
            setCurrentKey(in.domainId());
            this.cepEventLog.append(veOut.cepStateModifications());
        }
    }

    /**
     * Note that this must be called inside an open method of a flink rich thing
     */
    @Override
    protected OrderPreservingAsyncExecutor.UserFnPort<Envelope<CepState, Msg>, Envelope<CepState, Msg>, Output<StreamRecord<Envelope<CepState, Msg>>>> createUserFnPort(AppContainer<ESC, CepState, Msg, Schema, RuntimeContext, Output<StreamRecord<Envelope<CepState, Msg>>>, FlinkMetricsParams> container) {
        return (frTypeClass, env, corrId, completion) -> {
            Objects.requireNonNull(cepEventLog, "MakeEmptyValueEnvelopeWithCepStateFunction not opened");
            if (env instanceof ValueEnvelope<CepState, Msg> ve) {

                long start = timeService.currentTimeNanos();
                var afterParse = parser.parse(env);
                var afterEnrichment = enrichmentPipelineStep.process(afterParse);
                var afterBizLogic = bizLogic.process(afterEnrichment);
                envelopeMetrics.addToMetricsAtEnd(afterBizLogic);
                long finish = timeService.currentTimeNanos();
                long duration = finish - start;
                metrics.histogram("NormalPipelineFunction.asyncInvoke.millis", duration);
                afterBizLogic.valueEnvelope().setDurationNanos(duration);
                var updated = afterBizLogic.map(e -> {
                    cepEventLog.append(e.cepStateModifications());
                    return e;
                });
                completion.success(env, corrId, updated);
            } else {
                completion.success(env, corrId, env);

            }
        };
    }


    @Override
    protected void protectedSetupInOpen(AppContainer<ESC, CepState, Msg, Schema, RuntimeContext, Output<StreamRecord<Envelope<CepState, Msg>>>, FlinkMetricsParams> container) {
        this.parser = new ParseMessagePipelineStep<>(container);
        this.enrichmentPipelineStep = new EnrichmentPipelineStep<>(container, module);
        this.bizLogic = new BizLogicPipelineStep<>(container, null, module);
        var metricsFactory = container.metricsFactory();
        var params = FlinkMetricsParams.fromRuntime(getRuntimeContext(), getClass());
        this.metrics = metricsFactory.create(params);
        this.envelopeMetrics = EnvelopeMetrics.create(container.timeService(), metrics, EnvelopeMetricsTC.INSTANCE);
        this.timeService = container.timeService();
        this.cepStateTypeClass = container.cepStateTypeClass();
        this.cepEventLog = container.eventLogFromRuntimeContext().apply(getRuntimeContext());
        this.setKey = createSetKey();
    }

}

