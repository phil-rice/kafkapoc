package com.hcltech.rmg.cepstate;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface CepEventLog {
    void append(Collection<CepEvent> batch) throws Exception;

    List<CepEvent> getAll() throws Exception;

    default Map<String, Object> foldAll(Map<String, Object> initialState) throws Exception {
        var state = initialState;
        for (var event : getAll())
            state = event.fold(state);
        return state;
    }
}
