package com.hcltech.rmg.execution.bizlogic;

import com.hcltech.rmg.celcore.CelRuleBuilderFactory;
import com.hcltech.rmg.config.bizlogic.BizLogicAspect;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.configs.Configs;
import com.hcltech.rmg.execution.aspects.AspectExecutor;
import com.hcltech.rmg.execution.aspects.AspectExecutorRepository;
import com.hcltech.rmg.messages.ValueEnvelope;

public class BizLogicExecutor<CepState, Msg> implements AspectExecutor<BizLogicAspect, ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> {
    private final AspectExecutor<BizLogicAspect, ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> executor;
    AspectExecutorRepository<BizLogicAspect, ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> aspectRepository = new AspectExecutorRepository<>();

    public BizLogicExecutor(Configs configs, CelRuleBuilderFactory factory, Class<Msg> msgClass) {
        aspectRepository.register(CelInlineLogic.class, CelInlineLogicExecutor.create(factory, configs, msgClass));
        this.executor = aspectRepository.build();
    }

    @Override
    public ValueEnvelope<CepState, Msg> execute(String key, BizLogicAspect aspect, ValueEnvelope<CepState, Msg> input) {
        return executor.execute(key, aspect, input);
    }

}
