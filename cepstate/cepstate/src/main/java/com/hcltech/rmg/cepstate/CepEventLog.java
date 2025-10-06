package com.hcltech.rmg.cepstate;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface CepEventLog {
    void append(Collection<CepEvent> batch) throws Exception;

    List<CepEvent> getAll() throws CepEventException;

    default ErrorsOr<List<CepEvent>> safeGetAll() {
        try {
            return ErrorsOr.lift(getAll());
        } catch (CepEventException e) {
            return ErrorsOr.error(e.getCause().getClass().getSimpleName() + ": " + e.getCause().getMessage());
        }
    }


    default Map<String, Object> foldAll(Map<String, Object> initialState) throws CepEventException {
        var state = initialState;
        for (var event : getAll())
            state = event.fold(state);
        return state;
    }

    default ErrorsOr<Map<String, Object>> safeFoldAll(Map<String, Object> initialState) {
        try {
            return ErrorsOr.lift(foldAll(initialState));
        } catch (CepEventException e) {
            return ErrorsOr.error(e.getCause().getClass().getSimpleName() + ": " + e.getCause().getMessage());
        }
    }
}
