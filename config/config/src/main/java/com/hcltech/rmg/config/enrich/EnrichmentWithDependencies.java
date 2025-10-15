package com.hcltech.rmg.config.enrich;

import java.util.List;

public sealed interface EnrichmentWithDependencies
        permits MapLookupEnrichment {
    List<List<String>> inputs();
    List<String> output();
}

