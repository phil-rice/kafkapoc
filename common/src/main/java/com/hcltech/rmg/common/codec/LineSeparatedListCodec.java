// src/main/java/com/example/kafka/common/LineSeparatedListCodec.java
package com.hcltech.rmg.common.codec;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class LineSeparatedListCodec<T> implements Codec<List<T>, String> {
    private final Codec<T, String> item;

    public LineSeparatedListCodec(Codec<T, String> item) {
        this.item = Objects.requireNonNull(item, "item");
    }

    @Override
    public String encode(List<T> from) throws Exception {
        if (from == null || from.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < from.size(); i++) {
            if (i > 0) sb.append('\n');
            sb.append(item.encode(from.get(i)));
        }
        return sb.toString();
    }

    @Override
    public List<T> decode(String to) throws Exception {
        if (to == null || to.isEmpty()) return List.of();
        // Keep trailing empties; then drop one trailing newline (common file ending)
        String[] lines = to.split("\n", -1);
        int n = lines.length;
        if (n > 0 && lines[n - 1].isEmpty()) n -= 1;

        List<T> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(item.decode(lines[i]));
        }
        return out;
    }
}
