package com.hcltech.rmg.celcore;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.Map;

/** Executes a compiled expression over {input, context} maps. */
public interface RuleExecutor<Inp, Out> {
  ErrorsOr<Out> execute(Inp inp, Map<String, ?> context);
}
