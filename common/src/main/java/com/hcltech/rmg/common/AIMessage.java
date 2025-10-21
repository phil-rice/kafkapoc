package com.hcltech.rmg.common;

import java.util.Map;

public record AIMessage(Map<String, Object> input, Map<String, Object> output) {
}
