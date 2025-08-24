package com.example.cepstate.worklease;

import java.time.Instant;

public record MessageRecord(
        String domainId,
        long offset
) {}