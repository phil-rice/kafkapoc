package com.example.kafka.common.testevent;

// two subtypes we'll register by name
public record AlphaEvent(String path, Object value) implements TestEvent {
}
