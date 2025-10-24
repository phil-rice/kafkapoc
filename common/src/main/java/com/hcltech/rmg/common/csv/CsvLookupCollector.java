package com.hcltech.rmg.common.csv;

import java.util.*;

/**
 * Stream-friendly collector:
 *   1) new CsvLookupCollector(header, inputCols, outputCols, delimiter)
 *   2) accept(row)  // for each data row (NOT including header)
 *   3) build() -> CsvLookup  // contains Map<String, List<String>> and lookupToMap(...)
 */
public final class CsvLookupCollector {
    private final List<String> header;
    private final List<String> inputCols;
    private final List<String> outputCols;
    private final String delimiter;

    // column indices resolved once
    private final int[] inputIdx;
    private final int[] outputIdx;

    // streaming store: compositeKey -> values aligned with outputCols
    private final Map<String, List<String>> map;

    /**
     * @param header      first row (column names); NOT part of data
     * @param inputCols   columns that form the key, in order
     * @param outputCols  columns that form the value list, in order
     * @param delimiter   joiner for the key (e.g., ".")
     */
    public CsvLookupCollector(List<String> header,
                              List<String> inputCols,
                              List<String> outputCols,
                              String delimiter) {
        if (header == null || header.isEmpty()) throw new IllegalArgumentException("header cannot be null/empty");
        if (inputCols == null || inputCols.isEmpty()) throw new IllegalArgumentException("inputCols cannot be null/empty");
        if (outputCols == null || outputCols.isEmpty()) throw new IllegalArgumentException("outputCols cannot be null/empty");
        if (delimiter == null) throw new IllegalArgumentException("delimiter cannot be null");

        ensureUnique(header);

        this.header = header;
        this.inputCols = inputCols;
        this.outputCols = outputCols;
        this.delimiter = delimiter;

        Map<String, Integer> headerIndex = index(header);
        // validate & cache indices
        this.inputIdx = toIndices(inputCols, headerIndex, "input");
        this.outputIdx = toIndices(outputCols, headerIndex, "output");

        // size hint optional; you can pass an expected size via another ctor if you know row count
        this.map = new HashMap<>();
    }

    /** Optional constructor with expected size to reduce rehashing. */
    public CsvLookupCollector(List<String> header,
                              List<String> inputCols,
                              List<String> outputCols,
                              String delimiter,
                              int expectedRows) {
        this(header, inputCols, outputCols, delimiter);
        int cap = (int) ((expectedRows / 0.75f) + 1);
        // replace default map with pre-sized one
        this.map.clear();
        this.map.putAll(new HashMap<>(cap)); // not great; better to assign directly—see below overload
    }

    /** Better overload: directly preset capacity. */
    public static CsvLookupCollector withCapacity(List<String> header,
                                                  List<String> inputCols,
                                                  List<String> outputCols,
                                                  String delimiter,
                                                  int expectedRows) {
        if (expectedRows < 0) throw new IllegalArgumentException("expectedRows < 0");
        ensureUnique(header);
        Map<String, Integer> headerIndex = index(header);
        int[] inIdx = toIndices(inputCols, headerIndex, "input");
        int[] outIdx = toIndices(outputCols, headerIndex, "output");
        int cap = (int) ((expectedRows / 0.75f) + 1);
        return new CsvLookupCollector(header, inputCols, outputCols, delimiter, inIdx, outIdx, new HashMap<>(cap));
    }

    // private ctor for withCapacity
    private CsvLookupCollector(List<String> header,
                               List<String> inputCols,
                               List<String> outputCols,
                               String delimiter,
                               int[] inputIdx,
                               int[] outputIdx,
                               Map<String, List<String>> backing) {
        this.header = header;
        this.inputCols = inputCols;
        this.outputCols = outputCols;
        this.delimiter = delimiter;
        this.inputIdx = inputIdx;
        this.outputIdx = outputIdx;
        this.map = backing;
    }

    /** Accept a single data row (NOT including the header). Last duplicate key wins. */
    public void accept(List<String> row) {
        // key
        String key = joinKey(row, inputIdx, delimiter);
        // values, ordered like outputCols
        List<String> vals = extract(row, outputIdx);
        map.put(key, vals);
    }

    /** Build the lookup result (map + metadata + helper lookup method). */
    public CsvLookup build() {
        return new CsvLookup(map, inputCols, outputCols, delimiter);
    }

    // ----- internals -----

    private static Map<String, Integer> index(List<String> header) {
        Map<String, Integer> idx = new HashMap<>(header.size() * 2);
        for (int i = 0; i < header.size(); i++) idx.put(header.get(i), i);
        return idx;
    }

    private static int[] toIndices(List<String> names, Map<String, Integer> idx, String kind) {
        int[] out = new int[names.size()];
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            Integer n = idx.get(name);
            if (n == null) throw new IllegalArgumentException("Unknown " + kind + " column: " + name);
            out[i] = n;
        }
        return out;
    }

    private static void ensureUnique(List<String> header) {
        Set<String> seen = new HashSet<>();
        for (String h : header) if (!seen.add(h)) throw new IllegalArgumentException("Duplicate header: " + h);
    }

    private static String joinKey(List<String> row, int[] keyIdx, String delimiter) {
        if (keyIdx.length == 0) return "";
        // Build without intermediate list
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keyIdx.length; i++) {
            if (i > 0) sb.append(delimiter);
            int idx = keyIdx[i];
            String v = (idx < row.size()) ? row.get(idx) : null;
            sb.append(v == null ? "" : v);
        }
        return sb.toString();
    }

    private static List<String> extract(List<String> row, int[] valueIdx) {
        List<String> vals = new ArrayList<>(valueIdx.length);
        for (int idx : valueIdx) {
            vals.add(idx < row.size() ? row.get(idx) : null);
        }
        return vals;
    }
}
