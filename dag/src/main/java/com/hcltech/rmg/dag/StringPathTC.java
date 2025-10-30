package com.hcltech.rmg.dag;

import java.util.List;

public final class StringPathTC implements PathTC<String> {
    private static final char SEP = '.';

    @Override public boolean isPrefix(String prefix, String full) {
        if (prefix.equals(full)) return true;
        if (prefix.isEmpty() || full.length() < prefix.length()) return false;
        if (!full.startsWith(prefix)) return false;
        return full.length() == prefix.length() || full.charAt(prefix.length()) == SEP;
    }

    @Override public int compare(String a, String b) {
        List<String> as = a.isEmpty() ? List.of() : List.of(a.split("\\.", -1));
        List<String> bs = b.isEmpty() ? List.of() : List.of(b.split("\\.", -1));
        if (as.size() != bs.size()) return Integer.compare(as.size(), bs.size()); // deeper > shallower
        for (int i = 0; i < as.size(); i++) {
            int c = as.get(i).compareTo(bs.get(i));
            if (c != 0) return c;
        }
        return 0;
    }
}
