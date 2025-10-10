package com.hcltech.rmg.cepstate;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.Collection;
import java.util.List;

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


    default <CepState> Object foldAll(CepStateTypeClass<CepState> tc, CepState initialState) throws CepEventException {
        return CepEvent.foldAll(tc, initialState, getAll());
    }

    default <CepState> ErrorsOr<Object> safeFoldAll(CepStateTypeClass<CepState> tc, CepState initialState) {
        try {
            return ErrorsOr.lift(foldAll(tc, initialState));
        } catch (CepEventException e) {
            return ErrorsOr.error(e.getCause().getClass().getSimpleName() + ": " + e.getCause().getMessage());
        }
    }
}
