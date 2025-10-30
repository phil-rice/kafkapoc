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

    static <CepState> CepState foldAll(CepStateTypeClass<CepState> tc, CepState initial, Collection<CepEvent> events) {
        var state = initial;
        for (var event : events)
            state = tc.processState(state, event);
        return state;
    }

    static Codec<CepEvent, String> codec = Codec.clazzCodec(CepEvent.class);

    static CepEvent set(List<String> path, Object value) {
        return new CepSetEvent(path, value);
    }

    static CepEvent append(List<String> path, Object value) {
        return new CepAppendEvent(path, value);
    }
}

