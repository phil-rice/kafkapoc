package com.hcltech.rmg.consumer.abstraction;

import java.util.Set;

public interface AssignmentListener<S> {
    void onAssigned(Set<S> shards);
    void onRevoked(Set<S> shards);
}
