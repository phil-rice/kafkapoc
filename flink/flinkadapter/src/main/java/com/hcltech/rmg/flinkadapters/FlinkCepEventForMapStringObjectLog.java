package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepEventException;
import com.hcltech.rmg.cepstate.CepEventLog;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Collection;

public final class FlinkCepEventForMapStringObjectLog implements CepEventLog {

    /**
     * Append-only log: no reads in hot path
     */
    private final ListState<CepEvent> log;

    /**
     * (Optional) last sequence/timestamp to guard idempotency
     */
    private final ValueState<Long> lastSeq;  // or lastTs, etc.

    // Reuse TypeInformation statically
    private static final TypeInformation<CepEvent> CEP_EVENT_TI =
            TypeInformation.of(CepEvent.class);

    public static FlinkCepEventForMapStringObjectLog from(RuntimeContext ctx, String namePrefix) {
        return new FlinkCepEventForMapStringObjectLog(ctx, namePrefix);
    }

    private FlinkCepEventForMapStringObjectLog(RuntimeContext ctx, String namePrefix) {
        ListStateDescriptor<CepEvent> logDesc =
                new ListStateDescriptor<>(namePrefix + ".log", CEP_EVENT_TI);
        this.log = ctx.getListState(logDesc);

        // Optional: keep a minimal “anchor” so append() can be idempotent/order-aware without scanning
        ValueStateDescriptor<Long> lastSeqDesc =
                new ValueStateDescriptor<>(namePrefix + ".lastSeq", Long.class);
        this.lastSeq = ctx.getState(lastSeqDesc);
    }

    /**
     * Hot path: *only writes*. No log.get() anywhere.
     */
    @Override
    public void append(Collection<CepEvent> batch) throws CepEventException {
        if (batch == null || batch.isEmpty()) return;
        try {
            // If you need idempotency or ordering,
            // check/advance lastSeq here *without* reading the log.
            // Long cur = lastSeq.value();
            // filter batch by seq > cur, then:
            for (CepEvent e : batch) {
                if (e != null) log.add(e);
            }
            // lastSeq.update(newMaxSeqFrom(batch));
        } catch (Exception e) {
            throw new CepEventException(e);
        }
    }

    /**
     * Avoid using this on the hot path; read only when you truly need a full dump (debug/periodic).
     */
    @Override
    public java.util.List<CepEvent> getAll() throws CepEventException {
        try {
            java.util.ArrayList<CepEvent> out = new java.util.ArrayList<>();
            for (CepEvent e : log.get()) out.add(e);
            return out;
        } catch (Exception e) {
            throw new CepEventException(e);
        }
    }

    // Convenience if you append single events frequently (avoids making a temporary list)
    public void appendOne(CepEvent e) throws CepEventException {
        if (e == null) return;
        try {
            log.add(e);  // still write-only
        } catch (Exception ex) {
            throw new CepEventException(ex);
        }
    }
}
