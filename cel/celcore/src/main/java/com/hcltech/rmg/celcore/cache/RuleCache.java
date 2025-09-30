package com.hcltech.rmg.celcore.cache;

import com.hcltech.rmg.celcore.CompiledRule;
import com.hcltech.rmg.common.errorsor.ErrorsOr;

/** Thread-safe cache for compiled rules keyed by a caller-provided string. */
public interface    RuleCache<Inp, Out> {
  /**
   * Get (or compile & cache) the rule for a key + source.
   * Implementations may cache failures too.
   */
  ErrorsOr<CompiledRule<Inp, Out>> get(String key);
}
