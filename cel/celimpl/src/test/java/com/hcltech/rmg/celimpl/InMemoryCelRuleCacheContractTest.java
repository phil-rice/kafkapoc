package com.hcltech.rmg.celimpl;

import com.hcltech.rmg.celcore.CelVarType;
import com.hcltech.rmg.celcore.CompiledCelRuleWithDetails;
import com.hcltech.rmg.celcore.cache.AbstractCelRuleCacheContractTest;
import com.hcltech.rmg.celcore.cache.InMemoryCelRuleCache;
import com.hcltech.rmg.celcore.cache.PopulateCelRuleCache;
import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.Map;
import java.util.function.Function;

/** Concrete contract runner for the in-memory cache using the CEL builder available in this module. */
class InMemoryCelRuleCacheContractTest extends AbstractCelRuleCacheContractTest {

    @Override
    protected Function<String, ErrorsOr<CompiledCelRuleWithDetails<Map<String, ?>, Integer>>> intCompiler() {
        return src -> CelRuleBuilders.newRuleBuilder
                .<Map<String, ?>, Integer>createCelRuleBuilder(src)
                .withVar("data", CelVarType.DYN, in -> in)                // expose single input as "data"
                .withResultCoercer((in, o) -> ((Number) o).intValue())    // coerce to Integer
                .compile();                                               // returns WithDetails
    }

    @Override
    protected PopulateCelRuleCache<Map<String, ?>, Integer> newCache(
            Function<String, ErrorsOr<CompiledCelRuleWithDetails<Map<String, ?>, Integer>>> compileFn,
            boolean overwriteOnPopulate) {
        return new InMemoryCelRuleCache<>(compileFn, overwriteOnPopulate);
    }
}
