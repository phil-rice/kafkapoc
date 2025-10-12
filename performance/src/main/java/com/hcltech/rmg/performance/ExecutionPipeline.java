package com.hcltech.rmg.performance;

import com.hcltech.rmg.all_execution.AllBizLogic;
import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.appcontainer.interfaces.InitialEnvelopeServices;
import com.hcltech.rmg.cepstate.CepEventLog;
import com.hcltech.rmg.config.configs.Configs;
import com.hcltech.rmg.flinkadapters.FlinkCepEventForMapStringObjectLog;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.InitialEnvelopeFactory;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.function.Supplier;

public class ExecutionPipeline<Msc, CepState, Msg, Schema> extends RichMapFunction<Envelope<CepState, Msg>, Envelope<CepState, Msg>> {
    private final String containerId;
    private final String module;
    private final Class<IAppContainerFactory<Msc, CepState, Msg, Schema>> factoryClass;
    private AllBizLogic<CepState, Msg> bizLogic;

    public ExecutionPipeline(Class<IAppContainerFactory<Msc, CepState, Msg, Schema>> factoryClass, String containerId, String module) {
        this.factoryClass = factoryClass;
        this.containerId = containerId;
        this.module = module;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        var container = IAppContainerFactory.resolve(factoryClass, containerId).valueOrThrow();
        this.bizLogic = container.bizLogic();
    }

    @Override
    public Envelope<CepState, Msg> map(Envelope<CepState, Msg> ve) throws Exception {
        if (ve instanceof ValueEnvelope<CepState, Msg> valueEnvelope) {
            var paramKey = ve.header().parameters().key();
            var event = ve.header().eventType();
            var fullKey = Configs.composeKey(paramKey, event, module);
            var specificConfig = ve.header().config().events().get(event);
            if (specificConfig == null)
                throw new IllegalStateException("No event config for event: " + event + ", available events: " + ve.header().config().events().keySet());
            var bizLogicForModule = specificConfig.bizlogic().get(module);
            var result = bizLogic.bizLogicExecutor().execute(fullKey, bizLogicForModule, valueEnvelope);
            return result;
        }
        return ve;
    }
}
