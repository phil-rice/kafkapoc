package com.hcltech.rmg.interfaces.bizlogic;


import com.hcltech.rmg.interfaces.outcome.Outcome;

import java.util.List;
import java.util.concurrent.CompletionStage;

public interface IOneToManyPipeline<Context extends IHasValueTc<From>, From, To> {
    String name();//Must be unique
    CompletionStage<Outcome<List<To>>> process(Context context, From from);
}
