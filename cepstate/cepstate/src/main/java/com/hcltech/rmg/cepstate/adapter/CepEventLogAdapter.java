package com.hcltech.rmg.cepstate.adapter;

import com.hcltech.rmg.cepstate.CepEvent;
import com.hcltech.rmg.cepstate.CepEventException;
import com.hcltech.rmg.cepstate.CepEventLog;

import java.util.Collection;
import java.util.List;

public interface CepEventLogAdapter {
    void appendEvents(Collection<CepEvent> events) throws CepEventException;
    List<CepEvent> readAllEvents() throws CepEventException;
    CepEventLog getBackend();
}
