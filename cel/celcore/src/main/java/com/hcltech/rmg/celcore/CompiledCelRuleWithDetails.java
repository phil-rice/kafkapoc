package com.hcltech.rmg.celcore;

public interface CompiledCelRuleWithDetails<Inp, Out> extends CompiledCelRule<Inp, Out> {
    String source();

    /**
     * Return the paths used for a particular variable name (e.g., "state", "data", "context").
     */
    java.util.List<String> pathsFor(String varName);
}
