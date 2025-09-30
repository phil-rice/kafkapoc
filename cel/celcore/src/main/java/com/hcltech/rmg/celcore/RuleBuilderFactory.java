package com.hcltech.rmg.celcore;

public interface RuleBuilderFactory {
    <Inp, Out> RuleBuilder<Inp, Out> newRuleBuilder(String source);
}
