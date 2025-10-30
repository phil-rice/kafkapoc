package com.hcltech.rmg.common.testevent;

public record BetaEvent(String path, Object value) implements TestEvent {
}
