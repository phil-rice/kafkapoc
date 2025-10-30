package com.hcltech.rmg.common.csv;

import java.util.*;

/**
 * Result of CsvLookupCollector.build():
 * - Holds the map and the column metadata.
 * - Provides lookupToMap(List<String> keyValues) -> Map<String,Object> (or null if not found).
 */
public final class CsvLookup {
    private final Map<String, List<String>> map; // compositeKey -> values (ordered like outputCols)
    private final List<String> inputCols;
    private final List<String> outputCols;
    private final String delimiter;

    CsvLookup(Map<String, List<String>> map,
              List<String> inputCols,
              List<String> outputCols,
              String delimiter) {
        this.map = Objects.requireNonNull(map, "map");
        this.inputCols = Objects.requireNonNull(inputCols, "inputCols");
        this.outputCols = Objects.requireNonNull(outputCols, "outputCols");
        this.delimiter = Objects.requireNonNull(delimiter, "delimiter");
    }

    public Map<String, List<String>> map() { return map; }
    public List<String> inputColumns() { return inputCols; }
    public List<String> outputColumns() { return outputCols; }
    public String delimiter() { return delimiter; }

    /** Build composite key from keyValues (in the SAME order as inputCols). */
    private String keyFrom(List<String> keyValues) {
        if (keyValues == null) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < inputCols.size(); i++) {
            if (i > 0) sb.append(delimiter);
            String v = (i < keyValues.size()) ? keyValues.get(i) : null;
            sb.append(v == null ? "" : v);
        }
        return sb.toString();
    }

    /**
     * Lookup and return a map from output column name to value.
     * @param keyValues values in the SAME order as inputCols
     * @return Map<String,Object> or null if key not found. Values may be null.
     */
    public Map<String, Object> lookupToMap(List<String> keyValues) {
        String key = keyFrom(keyValues);
        List<String> vals = map.get(key);
        if (vals == null) return null;

        Map<String, Object> out = new HashMap<>(outputCols.size() * 2);
        for (int i = 0; i < outputCols.size(); i++) {
            String col = outputCols.get(i);
            String v = i < vals.size() ? vals.get(i) : null;
            out.put(col, v);
        }
        return out;
    }
}
