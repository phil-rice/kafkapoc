package com.hcltech.rmg.cepstate.adapter;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepEventException;
import com.hcltech.rmg.cepstate.CepEventLog;
import com.hcltech.rmg.cepstate.InMemoryCepEventLog;

import java.util.Collection;
import java.util.List;

public final class InMemoryCepEventLogAdapter implements CepEventLogAdapter {

    private final InMemoryCepEventLog log = new InMemoryCepEventLog();

    @Override
    public void appendEvents(Collection<CepEvent> events) throws CepEventException {
        log.append(events);
    }

    @Override
    public List<CepEvent> readAllEvents() throws CepEventException {
        return log.getAll();
    }

    @Override
    public CepEventLog getBackend() {
        return log;
    }
}
