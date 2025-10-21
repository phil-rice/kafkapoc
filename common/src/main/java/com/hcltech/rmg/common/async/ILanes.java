package com.hcltech.rmg.common.async;

/**
 * Power-of-two set of lanes, single-threaded (operator thread).
 * Routes an item T to the lane responsible for it.
 */
public interface ILanes<T> {

    /** Route this item to its lane (hashing/typeclass is internal to the implementation). */
    ILane<T> lane(T t);
}
