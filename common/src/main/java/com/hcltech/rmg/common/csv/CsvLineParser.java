package com.hcltech.rmg.common.csv;

import java.util.ArrayList;
import java.util.List;

/**
 * Minimal CSV line parser.
 * - Supports quoted fields and doubled quotes inside quoted fields.
 * - Configurable delimiter (default comma).
 */
public record CsvLineParser(char delimiter) {
    public CsvLineParser() {
        this(',');
    }

    public List<String> parse(String line) {
        List<String> out = new ArrayList<>();
        if (line == null) {
            out.add("");
            return out;
        }
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '"') {
                // Escaped quote inside quoted field ("")
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    cur.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (ch == delimiter && !inQuotes) {
                out.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(ch);
            }
        }
        out.add(cur.toString());
        return out;
    }
}
