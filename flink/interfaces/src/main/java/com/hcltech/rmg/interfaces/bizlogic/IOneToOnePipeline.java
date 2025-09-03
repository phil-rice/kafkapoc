package com.hcltech.rmg.interfaces.bizlogic;


import com.hcltech.rmg.interfaces.outcome.Outcome;

import java.util.concurrent.CompletionStage;

/** Our business logic implements this */
public interface IOneToOnePipeline<Context extends IHasValueTc<From>, From, To> {
    String name();//Must be unique
    CompletionStage<Outcome<To>> process(Context context, From from);
}
