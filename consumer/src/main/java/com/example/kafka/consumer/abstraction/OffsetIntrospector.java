// com.example.kafka.consumer.abstraction.OffsetIntrospector.java
package com.example.kafka.consumer.abstraction;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Optional capability: a stream that can reveal begin/end/committed offsets. */
public interface OffsetIntrospector<S, P> {
    Map<S, P> beginningOffsets(Set<S> shards);
    Map<S, P> endOffsets(Set<S> shards);
    default Optional<P> committedOffset(S shard) { return Optional.empty(); }
}
