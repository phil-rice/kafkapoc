package com.hcltech.rmg.dag;

import java.util.*;

/** Validates producer roots. Default policy: exact dup = error, prefix overlap = error. */
public interface ProducerValidation {

  public static <N, P> void validate(Set<N> nodes, PathTC<P> ptc, NodeTC<N, P> ntc) {
    Objects.requireNonNull(nodes); Objects.requireNonNull(ptc); Objects.requireNonNull(ntc);

    // deterministic for messages
    List<N> ordered = new ArrayList<>(nodes);
    ordered.sort(Comparator.comparing(ntc::label));

    // Map produced root -> first node that produced it
    Map<P, N> firstProducer = new LinkedHashMap<>();
    List<String> errors = new ArrayList<>();

    // Collect all roots (and detect exact duplicates)
    List<P> allRoots = new ArrayList<>();
    for (N n : ordered) {
      for (P root : ntc.owns(n)) {
        if (firstProducer.containsKey(root)) {
          errors.add("Duplicate produced root " + root + " by " + ntc.label(n)
                   + " (already produced by " + ntc.label(firstProducer.get(root)) + ")");
        } else {
          firstProducer.put(root, n);
          allRoots.add(root);
        }
      }
    }

    // Check prefix overlaps (a.b vs a.b.c)
    allRoots.sort(ptc::compare);
    for (int i = 0; i < allRoots.size(); i++) {
      for (int j = i + 1; j < allRoots.size(); j++) {
        P a = allRoots.get(i), b = allRoots.get(j);
        if (ptc.isPrefix(a, b) || ptc.isPrefix(b, a)) {
          N na = firstProducer.get(a), nb = firstProducer.get(b);
          errors.add("Overlapping produced roots " + a + " (" + ntc.label(na) + ") and "
                   + b + " (" + ntc.label(nb) + ")");
        }
      }
    }

    if (!errors.isEmpty()) {
      throw new IllegalArgumentException("Producer validation failed:\n - "
        + String.join("\n - ", errors));
    }
  }
}
