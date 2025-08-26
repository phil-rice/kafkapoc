package com.example.kafka.consumer.abstraction;

import java.util.Set;

public interface AssignmentListener<S> {
    void onAssigned(Set<S> shards);
    void onRevoked(Set<S> shards);
}
