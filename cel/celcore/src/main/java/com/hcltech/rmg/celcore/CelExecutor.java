package com.hcltech.rmg.celcore;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

public interface CelExecutor<Inp, Out> {
    Out execute(Inp input);
}
