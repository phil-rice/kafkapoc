package com.hcltech.rmg.cepstate;

import java.util.Collection;
import java.util.List;

/** No-op CEP log: never writes, always empty when read. */
public final class NullCepEventLog implements CepEventLog {
    @Override
    public void append(Collection<CepEvent> batch) throws CepEventException {
        // do nothing
    }

    @Override
    public List<CepEvent> getAll() throws CepEventException {
        return List.of();
    }
}
