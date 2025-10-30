package com.hcltech.rmg.common.csv;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public final class CsvResourceLoader {

    private CsvResourceLoader() {}

    public static CsvLookup load(String resourceName,
                                 List<String> inputColumns,
                                 List<String> outputColumns) {
        return load(resourceName, inputColumns, outputColumns, ',', ".");
    }

    public static CsvLookup load(String resourceName,
                                 List<String> inputColumns,
                                 List<String> outputColumns,
                                 char csvFieldDelimiter) {
        return load(resourceName, inputColumns, outputColumns, csvFieldDelimiter, ".");
    }

    public static CsvLookup load(String resourceName,
                                 List<String> inputColumns,
                                 List<String> outputColumns,
                                 char csvFieldDelimiter,
                                 String keyDelimiter) {

        // Null checks via Objects + minimal follow-up checks
        String rn = Objects.requireNonNull(resourceName, "resourceName");
        List<String> inCols = Objects.requireNonNull(inputColumns, "inputColumns");
        List<String> outCols = Objects.requireNonNull(outputColumns, "outputColumns");
        String kd = Objects.requireNonNull(keyDelimiter, "keyDelimiter");

        if (rn.isBlank()) throw new IllegalArgumentException("resourceName cannot be blank");
        if (inCols.isEmpty()) throw new IllegalArgumentException("inputColumns cannot be empty");
        if (outCols.isEmpty()) throw new IllegalArgumentException("outputColumns cannot be empty");
        // keyDelimiter could be empty string if you truly want no delimiter; keep as-is.

        InputStream is = tryLoad(rn);
        if (is == null) {
            throw new IllegalArgumentException("CSV resource not found on classpath: " + rn);
        }

        return loadFromInputStream(is, csvFieldDelimiter, rn, inCols, outCols, kd);
    }

    public static CsvLookup loadFromInputStream(InputStream is, char csvFieldDelimiter, String descriptionForError, List<String> inCols, List<String> outCols, String keyDelimiter) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            CsvLineParser lineParser = new CsvLineParser(csvFieldDelimiter);

            // first non-blank line = header
            List<String> header = null;
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) continue;
                line = stripBom(line);
                if (!line.isBlank()) {
                    header = lineParser.parse(line);
                    break;
                }
            }
            if (header == null || header.isEmpty()) {
                throw new IllegalArgumentException("CSV '" + descriptionForError + "' has no header row");
            }

            CsvLookupCollector collector = new CsvLookupCollector(header, inCols, outCols, keyDelimiter);

            // data lines
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                collector.accept(lineParser.parse(line));
            }

            return collector.build();
        } catch (RuntimeException re) {
            throw re; // preserve IllegalArgumentException etc.
        } catch (Exception e) {
            throw new RuntimeException("Error loading CSV resource: " + descriptionForError, e);
        }
    }

    private static InputStream tryLoad(String resourceName) {
        String normalized = resourceName.startsWith("/") ? resourceName.substring(1) : resourceName;
        ClassLoader ctx = Thread.currentThread().getContextClassLoader();
        InputStream is = (ctx != null) ? ctx.getResourceAsStream(normalized) : null;
        if (is != null) return is;
        return CsvResourceLoader.class.getClassLoader().getResourceAsStream(normalized);
    }

    private static String stripBom(String s) {
        return (s != null && !s.isEmpty() && s.charAt(0) == '\uFEFF') ? s.substring(1) : s;
    }
}
