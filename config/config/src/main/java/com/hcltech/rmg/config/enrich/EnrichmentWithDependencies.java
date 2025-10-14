package com.hcltech.rmg.config.enrich;

import java.util.List;
import java.util.Map;

public sealed interface EnrichmentWithDependencies
        permits FixedEnrichment {
    List<List<String>> inputs();
    List<String> output();
}

