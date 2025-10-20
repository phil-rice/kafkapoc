package com.hcltech.rmg.config.enrich;

import java.util.List;

public interface EnrichmentWithDependencies {
    List<List<String>> inputs();

    List<String> output();
}

