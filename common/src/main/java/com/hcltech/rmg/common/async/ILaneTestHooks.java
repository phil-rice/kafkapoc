package com.hcltech.rmg.common.async;

/**
 * Package-private hooks for unit tests of Lane/ILanes.
 */
interface ILaneTestHooks<T> {

    /** Directly inspect internal index state (for assertions only). */
    int  _headIdxForTest();
    int  _tailIdxForTest();
    int  _countForTest();
    int  _maskForTest();
    int  _depthForTest();

    /**
     * Check if any slot currently holds a reference equal to {@code t}.
     * Used only for verification in tests.
     */
    boolean _containsForTest(T t);

    /** Force-set the internal cursors and count for setup of edge-case tests. */
    void _setIdxForTest(int headIdx, int tailIdx, int count);
}
