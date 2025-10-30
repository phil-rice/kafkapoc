package com.hcltech.rmg.celcore;

public interface CompiledCelRule<Inp, Out> {
    CelExecutor<Inp, Out> executor();
}
