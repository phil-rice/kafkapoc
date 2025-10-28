package com.hcltech.rmg.flinkadapters;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepEventException;
import com.hcltech.rmg.cepstate.CepEventLog;
import com.hcltech.rmg.cepstate.adapter.CepEventLogAdapter;

import java.util.Collection;
import java.util.List;

public final class FlinkCepEventLogAdapter implements CepEventLogAdapter {

    private final FlinkCepEventForMapStringObjectLog flinkLog;

    public FlinkCepEventLogAdapter(FlinkCepEventForMapStringObjectLog flinkLog) {
        this.flinkLog = flinkLog;
    }

    @Override
    public void appendEvents(Collection<CepEvent> events) throws CepEventException {
        flinkLog.append(events);
    }

    @Override
    public List<CepEvent> readAllEvents() throws CepEventException {
        return flinkLog.getAll();
    }

    @Override
    public CepEventLog getBackend() {
        return flinkLog;
    }
}
