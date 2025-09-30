package com.hcltech.rmg.celcore;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.List;
import java.util.function.Function;

/**
 * Hexagonal port for building/compiling rules.
 * Engine-agnostic: no CEL types here.
 */
public interface RuleBuilder<Inp,Out> {

  /** Validate the list of input.* paths referenced by the rule. */
  RuleBuilder <Inp,Out>validateInput(Function<List<String>, List<String>> validator);

  /** Validate the list of context.* paths referenced by the rule. */
  RuleBuilder<Inp,Out> validateContext(Function<List<String>, List<String>> validator);

  /** Configure variable names (default: input/context). */
  RuleBuilder<Inp,Out> withVarNames(String inputVar, String contextVar);

  /** Optionally coerce the raw evaluation result (e.g. to Boolean). */
  RuleBuilder<Inp,Out> withResultCoercer(Function<Object, Object> coercer);

  /** Compile the rule and return an executor. Never throws. */
  ErrorsOr<CompiledRule<Inp,Out>> compile();
}
