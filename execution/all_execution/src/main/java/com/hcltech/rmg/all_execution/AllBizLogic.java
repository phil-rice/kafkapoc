package com.hcltech.rmg.all_execution;

import com.hcltech.rmg.celimpl.CelRuleBuilders;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.execution.bizlogic.BizLogicExecutor;

public record AllBizLogic<CepState, Msg>(BizLogicExecutor<CepState, Msg> bizLogicExecutor) {

    public static <CepState, Msg> AllBizLogic<CepState, Msg> create(BehaviorConfig config, Class<Msg> msgClass) {

        return new AllBizLogic<>(new BizLogicExecutor<>(config, CelRuleBuilders.newRuleBuilder, msgClass));
    }
}
