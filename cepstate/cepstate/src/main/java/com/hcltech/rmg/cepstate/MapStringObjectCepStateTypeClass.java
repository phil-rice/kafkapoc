package com.hcltech.rmg.cepstate;

import com.hcltech.rmg.common.Paths;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapStringObjectCepStateTypeClass implements CepStateTypeClass<Map<String, Object>> {


    @Override
    public Map<String, Object> createEmpty() {
        return new HashMap<>();
    }

    @Override
    public Map<String, Object> processState(Map<String, Object> state, CepEvent event) {
        if (event instanceof CepSetEvent setEvent) {
            return Paths.setValue(state, setEvent.path(), setEvent.value());
        } else if (event instanceof CepAppendEvent appendEvent) {
            return Paths.appendValue(state, appendEvent.path(), appendEvent.value());
        } else {
            throw new IllegalStateException("Unknown CepEvent type: " + event.getClass() + " " + event);
        }
    }

    @Override
    public Object getFromPath(Map<String, Object> cepState, List<String> path) {
        return Paths.getObject(cepState, path);
    }
}
