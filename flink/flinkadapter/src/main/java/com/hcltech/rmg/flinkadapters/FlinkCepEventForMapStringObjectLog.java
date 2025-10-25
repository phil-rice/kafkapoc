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

public final class FlinkCepEventForMapStringObjectLog implements CepEventLog {

    private final ListState<List<CepEvent>> segments;

    public static FlinkCepEventForMapStringObjectLog from(RuntimeContext ctx, String namePrefix) {
        return new FlinkCepEventForMapStringObjectLog(ctx, namePrefix);
    }

    private FlinkCepEventForMapStringObjectLog(RuntimeContext ctx, String namePrefix) {
        ListStateDescriptor<List<CepEvent>> desc = new ListStateDescriptor<>(
                namePrefix + ".segments",
                TypeInformation.of(new TypeHint<List<CepEvent>>() {
                })
        );
        this.segments = ctx.getListState(desc);
    }

    @Override
    public void append(Collection<CepEvent> batch) throws CepEventException {
        if (batch == null || batch.isEmpty()) return;
        // Defensive copy so callers can’t mutate after append
        try {
            segments.add(new ArrayList<>(batch));
        } catch (Exception e) {
            throw new CepEventException(e);
        }
    }

    @Override
    public List<CepEvent> getAll() throws CepEventException {
        List<CepEvent> out = new ArrayList<>();
        try {
            for (List<CepEvent> seg : segments.get()) {
                if (seg != null && !seg.isEmpty()) out.addAll(seg);
            }
            return out;
        } catch (Exception e) {
            throw new CepEventException(e);
        }
    }
}