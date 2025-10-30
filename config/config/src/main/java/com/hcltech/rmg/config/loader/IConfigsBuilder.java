package com.hcltech.rmg.config.loader;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.config.configs.Configs;

import java.util.List;
import java.util.function.Function;

@FunctionalInterface
public interface IConfigsBuilder {
    ErrorsOr<Configs> create(RootConfig root,
                             Function<List<String>, String> keyFn,
                             Function<List<String>, String> resourceFn,
                             ClassLoader cl);

    static IConfigsBuilder fromValue(Configs configs) {
        return (root, keyFn, resourceFn, cl) -> ErrorsOr.lift(configs);
    }
}
