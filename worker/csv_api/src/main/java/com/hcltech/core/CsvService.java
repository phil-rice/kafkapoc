package com.hcltech.rmg.csv_api.core;

import com.hcltech.rmg.common.csv.CsvLookup;
import com.hcltech.rmg.common.csv.CsvResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class CsvService {

    // cache lookups by a composite key: resource|inCols|outCols
    private final Map<String, CsvLookup> cache = new ConcurrentHashMap<>();
    private static final String BASE = "csv_api_csvs/"; // under classpath

    public Set<String> availableResources() {
        try {
            var resolver = new PathMatchingResourcePatternResolver();
            Resource[] resources = resolver.getResources("classpath:/" + BASE + "*.csv");
            Set<String> names = new HashSet<>();
            for (Resource r : resources) {
                String fn = Objects.requireNonNull(r.getFilename());
                names.add(fn.endsWith(".csv") ? fn.substring(0, fn.length() - 4) : fn);
            }
            return names;
        } catch (Exception e) {
            throw new RuntimeException("Failed listing csv_api_csvs", e);
        }
    }

    public List<String> lookup(String resourceName,
                               String key,
                               List<String> inputColumns,
                               List<String> outputColumns) {

        if (resourceName == null || resourceName.isBlank()) return List.of();

        // sensible defaults if not provided
        List<String> inCols = (inputColumns == null || inputColumns.isEmpty())
                ? List.of("key")
                : inputColumns;
        List<String> outCols = (outputColumns == null || outputColumns.isEmpty())
                ? List.of("value")
                : outputColumns;

        String cacheKey = resourceName + "|" + String.join(",", inCols) + "|" + String.join(",", outCols);
        CsvLookup lookup = cache.computeIfAbsent(cacheKey, k ->
                CsvResourceLoader.load(BASE + resourceName + ".csv", inCols, outCols)
        );

        Map<String, Object> result = lookup.lookupToMap(List.of(key));
        if (result == null) return List.of();

        List<String> out = new ArrayList<>(outCols.size());
        for (String col : outCols) {
            Object v = result.get(col);
            out.add(v == null ? null : v.toString());
        }
        return out;
    }
}
