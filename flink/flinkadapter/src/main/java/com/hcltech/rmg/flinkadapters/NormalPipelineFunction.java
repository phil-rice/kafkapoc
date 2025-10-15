package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.all_execution.BizLogicPipelineStep;
import com.hcltech.rmg.all_execution.EnrichmentPipelineStep;
import com.hcltech.rmg.all_execution.ParseMessagePipelineStep;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.messages.Envelope;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.List;


public class NormalPipelineFunction<MSC, CepState, Msg, Schema> extends RichAsyncFunction<Envelope<CepState, Msg>, Envelope<CepState, Msg>> {
    private final Class<IAppContainerFactory<MSC, CepState, Msg, Schema>> factoryClass;
    private final String containerId;
    private final String module;
    private EnrichmentPipelineStep<MSC, CepState, Msg, Schema> enrichmentPipelineStep;
    private BizLogicPipelineStep<MSC, CepState, Msg, Schema> bizLogic;
    private ParseMessagePipelineStep<MSC, CepState, Msg, Schema> parser;

    public NormalPipelineFunction(Class<IAppContainerFactory<MSC, CepState, Msg, Schema>> factoryClass, String containerId, String module) {
        this.factoryClass = factoryClass;
        this.containerId = containerId;
        this.module = module;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        var container = IAppContainerFactory.resolve(factoryClass, containerId).valueOrThrow();
        this.parser = new ParseMessagePipelineStep<>(container);
        this.enrichmentPipelineStep = new EnrichmentPipelineStep<>(container, module);
        this.bizLogic = new BizLogicPipelineStep<>(container, null, module);
    }

    @Override
    public void asyncInvoke(Envelope<CepState, Msg> cepStateMsgEnvelope, ResultFuture<Envelope<CepState, Msg>> resultFuture) throws Exception {
        var afterParse = parser.parse(cepStateMsgEnvelope);
        var afterEnrichment = enrichmentPipelineStep.process(afterParse);
        var afterBizLogic = bizLogic.process(afterEnrichment);
        resultFuture.complete(List.of(afterBizLogic));
    }
}

