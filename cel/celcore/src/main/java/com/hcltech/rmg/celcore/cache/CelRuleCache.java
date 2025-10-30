package com.hcltech.rmg.celcore.cache;

import com.hcltech.rmg.celcore.CompiledCelRule;

/**
 * Thread-safe cache for compiled rules keyed by a caller-provided string.
 */
public interface CelRuleCache<Inp, Out> {

    CompiledCelRule<Inp, Out> get(String key) throws CelRuleNotFoundException;
}
