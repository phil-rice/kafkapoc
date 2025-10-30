// com.hcltech.rmg.common.async.ILanesIntrospect.java
package com.hcltech.rmg.common.async;

import java.util.function.Consumer;

public interface ILanesIntrospect<T> {
    void forEachLane(Consumer<ILane<T>> consumer);
}
