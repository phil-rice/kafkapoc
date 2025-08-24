package com.example.kafka.common.testevent;

public record BetaEvent(String path, Object value) implements TestEvent {
}
