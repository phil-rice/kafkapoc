package com.hcltech.rmg.execution.aspects;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

public interface AspectExecutor <Component, Inp, Out>{
    ErrorsOr<Out> execute(String key,Component component, Inp input);
}
