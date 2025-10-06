package com.hcltech.rmg.cepstate;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.hcltech.rmg.common.codec.Codec;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,      // use a type name, not full class name
        include = JsonTypeInfo.As.PROPERTY,
        property = "type"                // JSON field to indicate which subclass
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = CepSetEvent.class, name = "set"),
        @JsonSubTypes.Type(value = CepAppendEvent.class, name = "append")
})
public interface CepEvent {

    Map<String, Object> fold(Map<String, Object> state);

    static Map<String,Object> foldAll(Map<String,Object> initial, Collection<CepEvent> events) {
        var state = initial;
        for(var event: events) {
            state = event.fold(state);
        }
        return state;
    }
    static Map<String,Object> foldMultiple(Map<String,Object> initial, Collection<Collection<CepEvent>> events) {
        var state = initial;
        for(var eventList: events) {
            state = foldAll(state, eventList);
        }
        return state;
    }

    static Codec<CepEvent, String> codex = Codec.clazzCodec(CepEvent.class);

    static CepEvent set(List<String> path, Object value) {
        return new CepSetEvent(path, value);
    }

    static CepEvent append(List<String> path, Object value) {
        return new CepAppendEvent(path, value);
    }
}

