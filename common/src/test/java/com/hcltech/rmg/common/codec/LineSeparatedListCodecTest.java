// src/test/java/com/hcltech/rmg/common/codec/LineSeparatedListCodecErrorsTest.java
package com.hcltech.rmg.common.codec;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class LineSeparatedListCodecTest {

    /** Item codec that encodes ints as strings, but returns an error for negatives. */
    static class IntItemCodecWithErrors implements Codec<Integer, String> {
        @Override public ErrorsOr<String> encode(Integer from) {
            if (from < 0) return ErrorsOr.error("negatives not allowed: " + from);
            return ErrorsOr.lift(String.valueOf(from));
        }
        @Override public ErrorsOr<Integer> decode(String to) {
            try {
                int v = Integer.parseInt(to);
                if (v < 0) return ErrorsOr.error("negatives not allowed: " + v);
                return ErrorsOr.lift(v);
            } catch (NumberFormatException nfe) {
                return ErrorsOr.error("not a number: " + to);
            }
        }
    }

    /** Item codec that throws to exercise the outer try/catch branch (unexpected exception). */
    static class ThrowingItemCodec implements Codec<Integer, String> {
        @Override public ErrorsOr<String> encode(Integer from) {
            throw new RuntimeException("kaboom-encode");
        }
        @Override public ErrorsOr<Integer> decode(String to) {
            throw new RuntimeException("kaboom-decode");
        }
    }

    @Test
    void encode_aggregates_errors_from_item_codec() {
        Codec<Integer, String> item = new IntItemCodecWithErrors();
        LineSeparatedListCodec<Integer> lines = new LineSeparatedListCodec<>(item);

        // Two errors (negatives) + one ok (1). Should return aggregated errors.
        var res = lines.encode(List.of(-1, 1, -2));
        assertTrue(res.isError());
        var errs = res.errorsOrThrow();
        assertEquals(2, errs.size());
        assertTrue(errs.get(0).contains("negatives not allowed: -1"));
        assertTrue(errs.get(1).contains("negatives not allowed: -2"));
    }

    @Test
    void decode_aggregates_errors_from_item_codec() {
        Codec<Integer, String> item = new IntItemCodecWithErrors();
        LineSeparatedListCodec<Integer> lines = new LineSeparatedListCodec<>(item);

        // Mix of good, bad, and negative (both produce errors)
        String wire = "10\nfoo\n-5\n20";
        var res = lines.decode(wire);
        assertTrue(res.isError());
        var errs = res.errorsOrThrow();
        assertEquals(2, errs.size());
        assertTrue(errs.get(0).contains("not a number: foo"));
        assertTrue(errs.get(1).contains("negatives not allowed: -5"));
    }

    @Test
    void encode_unexpected_exception_is_wrapped() {
        LineSeparatedListCodec<Integer> lines = new LineSeparatedListCodec<>(new ThrowingItemCodec());
        var res = lines.encode(List.of(1, 2));
        assertTrue(res.isError());
        String msg = res.errorsOrThrow().get(0);
        assertTrue(msg.startsWith("Failed to encode list: kaboom-encode"));
    }

    @Test
    void decode_unexpected_exception_is_wrapped() {
        LineSeparatedListCodec<Integer> lines = new LineSeparatedListCodec<>(new ThrowingItemCodec());
        var res = lines.decode("1\n2");
        assertTrue(res.isError());
        String msg = res.errorsOrThrow().get(0);
        assertTrue(msg.startsWith("Failed to decode list: kaboom-decode"));
    }

    @Test
    void null_inputs_are_handled() {
        Codec<Integer, String> intJson = Codec.clazzCodec(Integer.class);
        LineSeparatedListCodec<Integer> lines = new LineSeparatedListCodec<>(intJson);

        // encode(null) -> ""
        assertEquals("", lines.encode(null).valueOrThrow());

        // decode(null) -> []
        assertTrue(lines.decode(null).valueOrThrow().isEmpty());
    }

    @Test
    void constructor_rejects_null_item_codec() {
        assertThrows(NullPointerException.class, () -> new LineSeparatedListCodec<>(null));
    }
}
