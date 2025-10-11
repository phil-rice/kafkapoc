package com.hcltech.rmg.celimpl;

import com.hcltech.rmg.celcore.CelRuleBuilder;
import com.hcltech.rmg.celcore.CelRuleBuilderFactory;

public final class CelRuleBuilders {
    private CelRuleBuilders() {
    }

    /**
     * Factory: create a new builder for the given CEL source.
     */
    public static CelRuleBuilderFactory newRuleBuilder = DefaultCelRuleBuilder::new;
}
