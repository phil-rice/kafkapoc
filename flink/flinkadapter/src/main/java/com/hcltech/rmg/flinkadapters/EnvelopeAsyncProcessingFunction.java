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
import com.hcltech.rmg.common.copy.DeepCopy;
import com.hcltech.rmg.common.metrics.Metrics;
import com.hcltech.rmg.flink_metrics.FlinkMetricsParams;
import com.hcltech.rmg.messages.AiFailureEnvelopeFactory;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.EnvelopeHeader;
import com.hcltech.rmg.messages.ValueEnvelope;
import com.hcltech.rmg.common.metrics.EnvelopeMetrics;
import com.hcltech.rmg.metrics.EnvelopeMetricsTC;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Keyed operator that keeps CEP state and runs the OrderPreservingAsyncExecutor
 * in the same operator (no AsyncFunction). Completions are drained on the operator
 * thread and emitted directly to the Collector (no retention).
 * <p>
 * K   : Flink key type (String)
 * In  : Envelope<CepState, Msg>
 * Out : Envelope<CepState, Msg>
 */
public class EnvelopeAsyncProcessingFunction<ESC, CepState, Msg, Schema>
        extends AbstractEnvelopeAsyncProcessingFunction<ESC, CepState, Msg, Schema, RuntimeContext, FlinkMetricsParams> {


    private final String module;
    private final boolean rememberBizlogicInput;
    private ParseMessagePipelineStep<ESC, CepState, Msg, Schema, RuntimeContext, Output<StreamRecord<Envelope<CepState, Msg>>>, FlinkMetricsParams> parser;
    private EnrichmentPipelineStep<ESC, CepState, Msg, Schema, RuntimeContext, Output<StreamRecord<Envelope<CepState, Msg>>>, FlinkMetricsParams> enrichmentPipelineStep;
    private BizLogicPipelineStep<ESC, CepState, Msg, Schema, RuntimeContext, Output<StreamRecord<Envelope<CepState, Msg>>>, FlinkMetricsParams> bizLogic;
    private Metrics metrics;
    private EnvelopeMetrics<Envelope<?, ?>> envelopeMetrics;
    private ITimeService timeService;
    private CepStateTypeClass<CepState> cepStateTypeClass;
    private CepEventLog cepEventLog;
    private BiConsumer<Envelope<CepState, Msg>, Envelope<CepState, Msg>> setKey;
    private DeepCopy<Msg> msgDeepCopy;
    private DeepCopy<CepState> cesStateDeepCopy;
    private Function<Envelope<CepState, Msg>, Envelope<CepState, Msg>> afterParse;

    public EnvelopeAsyncProcessingFunction(AppContainerDefn appContainerDefn, String module, boolean rememberBizlogicInput) {
        super(appContainerDefn);
        this.module = module;
        this.rememberBizlogicInput = rememberBizlogicInput;
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
                var parsed = parser.parse(env);

                var afterEnrichment = enrichmentPipelineStep.process(parsed);
                if (rememberBizlogicInput) {
                    // Deep copy cep state and msg to store in cargo - used for AI pipelines to compare input vs output
                    var cepStateCopy = cesStateDeepCopy.copy(afterEnrichment.valueEnvelope().cepState());
                    var msgCopy = msgDeepCopy.copy(afterEnrichment.valueEnvelope().data());
                    EnvelopeHeader<CepState> header = afterEnrichment.valueEnvelope().header();
                    var cargo = new HashMap<>(header.cargo());
                    cargo.put(AiFailureEnvelopeFactory.BIZLOGIC_INPUT_CEP_STATE_CARGO_KEY, cepStateCopy);
                    cargo.put(AiFailureEnvelopeFactory.BIZLOGIC_INPUT_MSG_CARGO_KEY, msgCopy);
                    var newHeader = header.withCargo(cargo);
                    afterEnrichment.valueEnvelope().setHeader(newHeader);
                }

                var afterBizLogic = bizLogic.process(afterEnrichment);
                envelopeMetrics.addToMetricsAtEnd(afterBizLogic);
                long finish = timeService.currentTimeNanos();
                long duration = finish - start;
                metrics.histogram("NormalPipelineFunction.asyncInvoke.millis", duration);
                afterBizLogic.valueEnvelope().setDurationNanos(duration);

                completion.success(env, corrId, afterBizLogic);
            } else {
                completion.success(env, corrId, env);

            }
        };
    }


    @Override
    protected void protectedSetupInOpen(AppContainer<ESC, CepState, Msg, Schema, RuntimeContext, Output<StreamRecord<Envelope<CepState, Msg>>>, FlinkMetricsParams> container) {
        this.parser = new ParseMessagePipelineStep<>(container);
        this.afterParse = container.afterParse();
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
        this.cesStateDeepCopy = container.deepCopyCepState();
        this.msgDeepCopy = container.deepCopyMsg();
    }

}

