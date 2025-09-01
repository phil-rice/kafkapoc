package com.hcltech.rmg.consumer.abstraction;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Strategy for deciding where to start consuming on partition assignment.
 */
public interface SeekStrategy<S,P> {
    void onAssigned(SeekOps<S,P> ops, Set<S> shards);

    /** Continue from committed position (default). */
    static <S,P> SeekStrategy<S,P> continueCommitted() {
        return (ops, shards) -> { /* nothing */ };
    }

    /** Reset to beginning, but only once per shard. */
    static <S,P> SeekStrategy<S,P> fromBeginningOnce() {
        return new SeekStrategy<>() {
            private final Set<S> initialized = Collections.synchronizedSet(new HashSet<>());
            @Override
            public void onAssigned(SeekOps<S,P> ops, Set<S> shards) {
                Set<S> toReset = new HashSet<>();
                for (S s : shards) {
                    if (!initialized.contains(s)) {
                        toReset.add(s);
                        initialized.add(s);
                    }
                }
                if (!toReset.isEmpty()) {
                    ops.seekToBeginning(toReset);
                }
            }
        };
    }
}
