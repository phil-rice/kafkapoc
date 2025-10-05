package com.hcltech.rmg.common.uuid;

import java.util.UUID;

public interface IUuidGenerator {
    String generate();

    static IUuidGenerator defaultGenerator() {
        return () -> UUID.randomUUID().toString();
    }
}