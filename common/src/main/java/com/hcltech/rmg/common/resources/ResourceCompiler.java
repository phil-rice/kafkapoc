package com.hcltech.rmg.common.resources;

/** Compile-from-resource boundary (kept tiny & generic). */
@FunctionalInterface
public interface ResourceCompiler<T> {
    T compile(String resourceName) throws Exception;
}
