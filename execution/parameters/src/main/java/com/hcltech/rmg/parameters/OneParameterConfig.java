package com.hcltech.rmg.parameters;

import java.io.Serializable;
import java.util.List;

public record OneParameterConfig(List<String> legalValue, String defaultValue, String description) implements Serializable {
}
