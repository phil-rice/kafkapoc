package com.hcltech.rmg.celcore;

@FunctionalInterface
public interface CelRuleBuilderFactory {
    <Inp, Out> CelRuleBuilder<Inp, Out> createCelRuleBuilder(String source);
}
