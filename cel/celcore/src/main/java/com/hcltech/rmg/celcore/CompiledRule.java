package com.hcltech.rmg.celcore;

/** Result of a successful compile: metadata + an executor. */
public interface CompiledRule<Inp, Out> {
  String source();
  RuleUsage usage();
  RuleExecutor<Inp, Out> executor();
}
