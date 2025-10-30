package com.hcltech.rmg.parameters;

import java.io.Serializable;
import java.util.List;

public record ParameterConfig (List<OneParameterConfig> parameters) implements Serializable {
    public static ParameterConfig empty() {
        return new ParameterConfig(List.of());
    }
}
