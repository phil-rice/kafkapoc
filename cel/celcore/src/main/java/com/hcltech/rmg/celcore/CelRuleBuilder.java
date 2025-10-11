package com.hcltech.rmg.celcore;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

// Keep it small & focused.
public interface CelRuleBuilder<Inp, Out> {
  /** Declare a variable available in CEL (dyn today; proto/typed later). */
  CelRuleBuilder<Inp, Out> withVar(String name, CelVarType type, Function<Inp, Object> getter);

  /** Coerce/validate the raw CEL result into Out (e.g., ensure List<String>, Boolean, etc.). */
  CelRuleBuilder<Inp, Out> withResultCoercer(BiFunction<Inp,Object, Out> coercer);

  /** Fill the tiny activation map from your input per execution (fast path). */
  CelRuleBuilder<Inp, Out> withActivationFiller(BiConsumer<Inp, Map<String,Object>> filler);

  /** Compile to a reusable, threadsafe program. */
 ErrorsOr<CompiledCelRuleWithDetails<Inp, Out>> compile();
}

