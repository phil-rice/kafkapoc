package com.hcltech.rmg.interfaces.pipeline;


import com.hcltech.rmg.interfaces.outcome.Outcome;

import java.util.List;
import java.util.concurrent.CompletionStage;

public interface IOneToManyPipeline<From, To>  extends IAsyncPipeline<From,To>{
    CompletionStage<Outcome<List<To>>> process( From from);
}
