package com.hcltech.rmg.all_execution;

import com.hcltech.rmg.celcore.CelRuleBuilderFactory;
import com.hcltech.rmg.celimpl.CelRuleBuilders;
import com.hcltech.rmg.execution.bizlogic.AbstractCelInlineLogicExecutorContractTest;

public class CelInlineLogicExecutorContractTest  extends AbstractCelInlineLogicExecutorContractTest {

    @Override
    protected CelRuleBuilderFactory realFactory() {
        return CelRuleBuilders.newRuleBuilder;
    }
}
