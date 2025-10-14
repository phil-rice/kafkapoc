package com.hcltech.rmg.messages;

import com.hcltech.rmg.common.Paths;

import java.util.List;
import java.util.Map;

public class MapStringObjectAndListStringMsgTypeClass implements MsgTypeClass<Map<String, Object>, List<String>> {

    @Override
    public Object getValueFromPath(Map<String, Object> stringObjectMap, List<String> path) {
        return Paths.getObject(stringObjectMap, path);
    }
}
