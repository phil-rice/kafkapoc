package com.hcltech.rmg.execution.aspects;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

public interface AspectExecutor<Component, Inp, Out> {
    /**
     * Note that this can throw runtime errors as well
     *
     * @param key       - the key to identify the aspect
     * @param component - the configuration for the component
     * @param input     - the input to the aspect
     * @return Out - the output of the aspect
     */
    Out execute(String key, Component component, Inp input);
}
