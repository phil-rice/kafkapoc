    package com.hcltech.rmg.execution.aspects;

    /**
     * Synchronous aspect executor.
     * Note: Implementations may throw runtime exceptions.
     *
     * @param <Component> Component/config type (exact-class dispatched)
     * @param <Inp>       Input type
     * @param <Out>       Output type
     */
    public interface AspectExecutorSync<Component, Inp, Out> {
        /**
         * @param key       the key to identify the aspect
         * @param component the configuration for the component (must be non-null)
         * @param input     the input to the aspect
         * @return Out - the output of the aspect
         * @throws RuntimeException implementors may throw; callers should handle
         */
        Out execute(String key, Component component, Inp input);
    }
