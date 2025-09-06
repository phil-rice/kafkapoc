package com.hcltech.rmg.interfaces.pipeline;

import com.hcltech.rmg.interfaces.outcome.Outcome;

import java.util.List;

public interface IOneToOneSyncPipeline<From, To>extends ISyncPipeline<From,To> {
    Outcome<To> process(From from);
}
