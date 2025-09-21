package com.hcltech.rmg.interfaces.pipeline;

import com.hcltech.rmg.interfaces.outcome.Outcome;

import java.util.List;

public interface IOneToManySyncPipeline<From, To>extends ISyncPipeline<From,To> {
    Outcome<List<To>> process(From from);
}
