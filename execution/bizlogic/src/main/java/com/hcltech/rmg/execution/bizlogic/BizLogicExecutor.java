package com.hcltech.rmg.execution.bizlogic;

import com.hcltech.rmg.celcore.CelRuleBuilderFactory;
import com.hcltech.rmg.common.function.Callback;
import com.hcltech.rmg.config.bizlogic.BizLogicAspect;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.configs.Configs;
import com.hcltech.rmg.execution.aspects.AspectExecutorAsync;
import com.hcltech.rmg.execution.aspects.AspectExecutorRepository;
import com.hcltech.rmg.messages.ValueEnvelope;

public class BizLogicExecutor<CepState, Msg> implements AspectExecutorAsync<BizLogicAspect, ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> {
    private final AspectExecutorAsync<BizLogicAspect, ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> executor;
    AspectExecutorRepository<BizLogicAspect, ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> aspectRepository = new AspectExecutorRepository<>();

    public BizLogicExecutor(Configs configs, CelRuleBuilderFactory factory, Class<Msg> msgClass) {
        aspectRepository.register(CelInlineLogic.class, CelInlineLogicExecutor.create(factory, configs, msgClass));
        this.executor = aspectRepository.build();
    }


    @Override
    public void call(String key, BizLogicAspect aspect, ValueEnvelope<CepState, Msg> input, Callback<? super ValueEnvelope<CepState, Msg>> cb) {
        executor.call(key, aspect, input, cb);
    }
}
