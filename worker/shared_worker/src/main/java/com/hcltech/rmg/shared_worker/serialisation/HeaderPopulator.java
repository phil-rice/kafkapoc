package com.hcltech.rmg.shared_worker.serialisation;

import java.io.Serializable;

@FunctionalInterface
public interface HeaderPopulator<E> extends Serializable {
    void accept(E e, org.apache.kafka.common.header.Headers headers);
}