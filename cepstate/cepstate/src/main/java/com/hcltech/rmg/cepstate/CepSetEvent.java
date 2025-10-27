    package com.hcltech.rmg.cepstate;

    import com.fasterxml.jackson.annotation.JsonTypeName;
    import com.hcltech.rmg.common.Paths;

    import java.util.List;
    import java.util.Map;
    @JsonTypeName("set")
    public record CepSetEvent(List<String> path, Object value) implements CepEvent {

    }

