package com.hcltech.rmg.cepstate;

import java.util.List;

public interface CepStateTypeClass<CepState> {
    CepState processState(CepState cepState, CepEvent cepEvent);

    CepState createEmpty();

    Object getFromPath(CepState cepState, List<String> path);
}
