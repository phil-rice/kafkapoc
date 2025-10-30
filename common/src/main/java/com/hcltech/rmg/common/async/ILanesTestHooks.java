package com.hcltech.rmg.common.async;

/** Package-private hooks for unit tests only. */
interface ILanesTestHooks<T> {
    int _laneMask();
    int _laneDepth();
    int _laneCount();
    ILane< T> _laneAt(int index) ;
}
