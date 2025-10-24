package com.hcltech.rmg.execution.bizlogic;

import com.hcltech.rmg.celcore.CelRuleBuilderFactory;
import com.hcltech.rmg.config.bizlogic.BizLogicAspect;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.configs.Configs;
import com.hcltech.rmg.execution.aspects.AspectExecutorAsync;
import com.hcltech.rmg.execution.aspects.AspectExecutorRepository;
import com.hcltech.rmg.messages.ValueEnvelope;
import com.hcltech.rmg.common.function.Callback;

/**
 * Async-shaped BizLogic dispatcher. Current concrete implementation (CEL inline)
 * is synchronous but exposes an async callback API and invokes the callback inline.
 * If/when other biz-logic aspects become asynchronous, register them with registerAsync.
 */
public final class BizLogicExecutor<CepState, Msg>
        implements AspectExecutorAsync<BizLogicAspect, ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> {

    private final AspectExecutorAsync<BizLogicAspect, ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>> dispatcher;

    public BizLogicExecutor(Configs configs, CelRuleBuilderFactory factory, Class<Msg> msgClass) {
        var repo = new AspectExecutorRepository<BizLogicAspect, ValueEnvelope<CepState, Msg>, ValueEnvelope<CepState, Msg>>();

        // CelInlineLogicExecutor is already async-shaped; register directly as async.
        var celAsync = CelInlineLogicExecutor.<CepState, Msg>create(factory, configs, msgClass);
        repo.registerAsync(CelInlineLogic.class, celAsync);

        this.dispatcher = repo.build();
    }

    @Override
    public void call(String key,
                     BizLogicAspect aspect,
                     ValueEnvelope<CepState, Msg> input,
                     Callback<? super ValueEnvelope<CepState, Msg>> cb) {
        // Delegates to snapshot dispatcher (callbacks may fire inline for sync executors)
        dispatcher.call(key, aspect, input, cb);
    }
}
