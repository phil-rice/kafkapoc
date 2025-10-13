package com.hcltech.rmg.dag;

import java.util.List;
import java.util.Objects;

public final class ListPathTC implements PathTC<List<String>> {
    @Override public boolean isPrefix(List<String> prefix, List<String> full) {
        if (Objects.equals(prefix, full)) return true;
        if (prefix == null || full == null || prefix.isEmpty() || prefix.size() > full.size()) return false;
        for (int i = 0; i < prefix.size(); i++) {
            if (!Objects.equals(prefix.get(i), full.get(i))) return false;
        }
        return true;
    }

    @Override public int compare(List<String> a, List<String> b) {
        int da = (a == null) ? 0 : a.size();
        int db = (b == null) ? 0 : b.size();
        if (da != db) return Integer.compare(da, db); // deeper > shallower
        for (int i = 0; i < da; i++) {
            int c = String.valueOf(a.get(i)).compareTo(String.valueOf(b.get(i)));
            if (c != 0) return c;
        }
        return 0;
    }
}
