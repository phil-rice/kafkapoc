package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepEventException;
import com.hcltech.rmg.cepstate.CepEventLog;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * CEP event log backed by PUT-only RocksDB state (no MERGE).
 * - Stores up to MAX=6 items per key in a ring (0..5) using MapState.
 * - Writes are pure PUTs; reads materialize in O(6).
 */
public final class FlinkCepEventLogV2Ring implements CepEventLog {

    private static final int MAX = 6;

    // Slots 0..5 -> event (PUT-only)
    private final MapState<Integer, CepEvent> slots;

    // Next write index (0..5)
    private final ValueState<Integer> head;

    // Current number of items (0..6)
    private final ValueState<Integer> size;

    // Optional: last sequence/timestamp for idempotency/ordering
    private final ValueState<Long> lastSeq;

    // TypeInfos (helpful if CepEvent isn’t a strict POJO)
    private static final TypeInformation<CepEvent> CEP_EVENT_TI = TypeInformation.of(CepEvent.class);
    private static final TypeInformation<Integer> INT_TI = TypeInformation.of(Integer.class);
    private static final TypeInformation<Long> LONG_TI = TypeInformation.of(Long.class);

    public static FlinkCepEventLogV2Ring from(RuntimeContext ctx, String namePrefix) {
        return new FlinkCepEventLogV2Ring(ctx, namePrefix);
    }

    private FlinkCepEventLogV2Ring(RuntimeContext ctx, String prefix) {
        MapStateDescriptor<Integer, CepEvent> slotsDesc =
                new MapStateDescriptor<>(prefix + ".ring.slots", INT_TI, CEP_EVENT_TI);
        this.slots = ctx.getMapState(slotsDesc);

        ValueStateDescriptor<Integer> headDesc =
                new ValueStateDescriptor<>(prefix + ".ring.head", INT_TI);
        this.head = ctx.getState(headDesc);

        ValueStateDescriptor<Integer> sizeDesc =
                new ValueStateDescriptor<>(prefix + ".ring.size", INT_TI);
        this.size = ctx.getState(sizeDesc);

        ValueStateDescriptor<Long> lastSeqDesc =
                new ValueStateDescriptor<>(prefix + ".ring.lastSeq", LONG_TI);
        this.lastSeq = ctx.getState(lastSeqDesc);
    }

    @Override
    public void append(Collection<CepEvent> batch) throws CepEventException {
        if (batch == null || batch.isEmpty()) return;
        try {
            Integer h = head.value(); if (h == null) h = 0;
            Integer sz = size.value(); if (sz == null) sz = 0;

            // If you need idempotency/order, consult lastSeq here and filter batch.
            // Long cur = lastSeq.value();
            // ... filter ...
            for (CepEvent e : batch) {
                if (e == null) continue;
                slots.put(h, e);          // PUT (no MERGE)
                h = (h + 1) % MAX;
                if (sz < MAX) sz++;
            }

            head.update(h);
            size.update(sz);
            // lastSeq.update(newMaxSeqFrom(batch));
        } catch (Exception ex) {
            throw new CepEventException(ex);
        }
    }

    @Override
    public List<CepEvent> getAll() throws CepEventException {
        try {
            Integer h = head.value(); if (h == null) h = 0;
            Integer sz = size.value(); if (sz == null) sz = 0;
            if (sz == 0) return List.of();

            List<CepEvent> out = new ArrayList<>(sz);
            int start = (h - sz + MAX) % MAX;     // oldest
            for (int i = 0; i < sz; i++) {
                int idx = (start + i) % MAX;
                CepEvent e = slots.get(idx);
                if (e != null) out.add(e);
            }
            return out;
        } catch (Exception ex) {
            throw new CepEventException(ex);
        }
    }

    // Convenience for single appends
    public void appendOne(CepEvent e) throws CepEventException {
        if (e == null) return;
        append(List.of(e));
    }
}
