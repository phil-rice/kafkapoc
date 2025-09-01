package com.hcltech.rmg.common;

import java.time.Instant;

/** Example payload model. Replace with what your topic actually carries. */
public record Message(String id, String payload, Instant timestamp) {}
