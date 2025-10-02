// src/main/java/com/example/kafka/common/LineSeparatedListCodec.java
package com.hcltech.rmg.common.codec;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class LineSeparatedListCodec<T> implements Codec<List<T>, String> {
    private final Codec<T, String> item;

    public LineSeparatedListCodec(Codec<T, String> item) {
        this.item = Objects.requireNonNull(item, "item");
    }

    @Override
    public ErrorsOr<String> encode(List<T> from) {
        try {
            if (from == null || from.isEmpty()) return ErrorsOr.lift("");
            StringBuilder sb = new StringBuilder();
            var errors = new ArrayList<String>();
            for (int i = 0; i < from.size(); i++) {
                if (i > 0) sb.append('\n');
                ErrorsOr<String> encode = item.encode(from.get(i));
                if (encode.isError()) errors.addAll(encode.errorsOrThrow());
                else sb.append(encode.valueOrThrow());
            }
            if (errors.isEmpty()) {
                return ErrorsOr.lift(sb.toString());
            } else {
                return ErrorsOr.errors(errors);
            }
        } catch (Exception e) {
            return ErrorsOr.error("Failed to encode list: " + e.getMessage());
        }
    }

    @Override
    public ErrorsOr<List<T>> decode(String to) {
        try {

            if (to == null || to.isEmpty()) return ErrorsOr.lift(List.of());
            // Keep trailing empties; then drop one trailing newline (common file ending)
            String[] lines = to.split("\n", -1);
            int n = lines.length;
            if (n > 0 && lines[n - 1].isEmpty()) n -= 1;

            List<T> out = new ArrayList<>(n);
            List<String> errors = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                ErrorsOr<T> decode = item.decode(lines[i]);
                if (decode.isError())
                    errors.addAll(decode.errorsOrThrow());
                else
                    out.add(decode.valueOrThrow());
            }
            if (errors.isEmpty()) {
                return ErrorsOr.lift(out);
            } else {
                return ErrorsOr.errors(errors);
            }
        } catch (Exception e) {
            return ErrorsOr.error("Failed to decode list: " + e.getMessage());
        }
    }
}
