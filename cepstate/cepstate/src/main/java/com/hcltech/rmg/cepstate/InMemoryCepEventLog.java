package com.hcltech.rmg.cepstate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public final class InMemoryCepEventLog<CepState> implements CepEventLog {
    // Keep batches to avoid rewriting previous data; flatten on read.
    private final List<List<CepEvent>> segments = new ArrayList<>();

    @Override
    public void append(Collection<CepEvent> batch) {
        if (batch == null || batch.isEmpty()) return;
        // Defensive copy so caller can’t mutate after append
        segments.add(new ArrayList<>(batch));
    }


    @Override
    public List<CepEvent> getAll() {
        if (segments.isEmpty()) return Collections.emptyList();
        // Flatten into a new list; expose as unmodifiable
        int estimated = 0;
        for (List<CepEvent> seg : segments) estimated += seg.size();
        List<CepEvent> out = new ArrayList<>(estimated);
        for (List<CepEvent> seg : segments) out.addAll(seg);
        return Collections.unmodifiableList(out);
    }
}
