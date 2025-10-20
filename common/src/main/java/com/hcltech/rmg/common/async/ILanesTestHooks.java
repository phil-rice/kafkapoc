package com.hcltech.rmg.common.async;

/** Package-private hooks for unit tests only. */
interface ILanesTestHooks<FR,T> {
    int _laneMask();
    int _laneDepth();
    int _laneCount();
    ILane<FR, T> _laneAt(int index) ;
}
