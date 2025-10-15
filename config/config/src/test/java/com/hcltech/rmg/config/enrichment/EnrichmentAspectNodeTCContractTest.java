package com.hcltech.rmg.config.enrichment;

import com.hcltech.rmg.config.enrich.EnrichmentAspect;
import com.hcltech.rmg.config.enrich.EnrichmentWithDepedenciesNodeTc;
import com.hcltech.rmg.config.enrich.MapLookupEnrichment;
import com.hcltech.rmg.dag.NodeTC;
import com.hcltech.rmg.dag.NodeTCContractTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class EnrichmentAspectNodeTCContractTest
        extends NodeTCContractTest<EnrichmentAspect, List<String>> {

    @Override
    protected boolean allowsMultipleProducedRoots() {
        return false;
    }

    @Override
    protected NodeTC<EnrichmentAspect, List<String>> ntc() {
        return EnrichmentWithDepedenciesNodeTc.INSTANCE;
    }

    @Override
    protected List<String> p(String path) {
        return path.isEmpty() ? List.of() : Arrays.asList(path.split("\\.", -1));
    }

    @Override
    protected MapLookupEnrichment makeNode(String label, String[] producesDot, String[] requiresDot) {
        // Force single output (use first if provided, else a default)
        List<List<String>> inputs = new ArrayList<>();
        for (String r : requiresDot) inputs.add(p(r));
        List<String> output = (producesDot.length == 0) ? List.of("out") : p(producesDot[0]);
        return new MapLookupEnrichment(inputs, output, Map.of());
    }
}
