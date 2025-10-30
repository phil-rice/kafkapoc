package com.hcltech.rmg.common.csv;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CsvLineParserTest {

    @Test
    @DisplayName("null line -> [\"\"]")
    void nullLine() {
        CsvLineParser p = new CsvLineParser();
        assertEquals(List.of(""), p.parse(null));
    }

    @Test
    @DisplayName("empty line -> [\"\"]")
    void emptyLine() {
        CsvLineParser p = new CsvLineParser();
        assertEquals(List.of(""), p.parse(""));
    }

    @Test
    @DisplayName("simple comma-separated values")
    void simpleCsv() {
        CsvLineParser p = new CsvLineParser();
        assertEquals(List.of("a", "b", "c"), p.parse("a,b,c"));
    }

    @Test
    @DisplayName("empty fields between delimiters")
    void emptyFields() {
        CsvLineParser p = new CsvLineParser();
        assertEquals(List.of("a", "", "c"), p.parse("a,,c"));
        assertEquals(List.of("", "", ""), p.parse(",,"));
        assertEquals(List.of("a", ""), p.parse("a,"));
        assertEquals(List.of("", "a"), p.parse(",a"));
    }

    @Test
    @DisplayName("spaces are preserved (no trimming)")
    void spacesPreserved() {
        CsvLineParser p = new CsvLineParser();
        assertEquals(List.of(" a ", " b "), p.parse(" a , b "));
        assertEquals(List.of("  a", "b  "), p.parse("  a,b  "));
    }

    @Test
    @DisplayName("quoted field with comma inside")
    void quotedWithComma() {
        CsvLineParser p = new CsvLineParser();
        assertEquals(List.of("a,b", "c"), p.parse("\"a,b\",c"));
        assertEquals(List.of("c", "a,b"), p.parse("c,\"a,b\""));
        assertEquals(List.of("a,b", "c,d"), p.parse("\"a,b\",\"c,d\""));
    }

    @Test
    @DisplayName("escaped quotes inside quoted field")
    void escapedQuotes() {
        CsvLineParser p = new CsvLineParser();
        assertEquals(List.of("a\"b", "c"), p.parse("\"a\"\"b\",c"));
        assertEquals(List.of("he said \"hi\"", "x"),
                p.parse("\"he said \"\"hi\"\"\",x"));
        assertEquals(List.of("b\"c\"d"), p.parse("\"b\"\"c\"\"d\""));
    }

    @Test
    @DisplayName("quoted field boundaries")
    void quotedBoundaries() {
        CsvLineParser p = new CsvLineParser();
        assertEquals(List.of("a", "b"), p.parse("\"a\",\"b\""));
        assertEquals(List.of("", "b"), p.parse("\"\",b"));
        assertEquals(List.of("a", ""), p.parse("a,\"\""));
    }

    @Test
    @DisplayName("alternate delimiter: semicolon")
    void alternateDelimiter() {
        CsvLineParser p = new CsvLineParser(';');
        assertEquals(List.of("a", "b", "c"), p.parse("a;b;c"));
        // quoted field containing delimiter
        assertEquals(List.of("a;b", "c"), p.parse("\"a;b\";c"));
        // empty fields
        assertEquals(List.of("a", "", "c"), p.parse("a;;c"));
    }

    @Test
    @DisplayName("unbalanced quotes are tolerated: treat rest of line as quoted")
    void unbalancedQuotes() {
        CsvLineParser p = new CsvLineParser();
        // No closing quote: comma is treated as data, not a delimiter
        assertEquals(List.of("a,b"), p.parse("\"a,b"));
        // Starts quoted, contains delimiter, no closing quote, then delimiter: all in same field
        assertEquals(List.of("a,b,c"), p.parse("\"a,b,c"));
    }

    @Test
    @DisplayName("mixed cases")
    void mixedCases() {
        CsvLineParser p = new CsvLineParser();
        assertEquals(List.of("a", "b,c", "", "d\"e", "f"),
                p.parse("a,\"b,c\",,\"d\"\"e\",f"));
    }
}
