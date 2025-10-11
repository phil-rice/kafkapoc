package com.hcltech.rmg.celcore.cache;

import com.hcltech.rmg.celcore.CompiledCelRuleWithDetails;
import com.hcltech.rmg.common.errorsor.ErrorsOr;

public interface PopulateCelRuleCache <Inp,Out> extends CelRuleCache<Inp,Out>{
    ErrorsOr<CompiledCelRuleWithDetails<Inp,Out>> populate(String key, String source);
}
