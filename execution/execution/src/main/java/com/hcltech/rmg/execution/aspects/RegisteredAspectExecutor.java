package com.hcltech.rmg.execution.aspects;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.List;


/** By the time we get here we know the module and aspect. We have crunched all the config down to a single tool (which might be a composed applicative so in the general
 * case it might be multiple, but as much as possible we want one)
 *
 * The key is the combination of the paramters that got us here. e,g, productname, company, eventtype.
 *
 * @param <Inp>
 * @param <Out>
 */
public interface RegisteredAspectExecutor<Component,Inp, Out> {
    ErrorsOr<Out> execute(String key,List<String> modules, String aspect, Component component, Inp input);
}
