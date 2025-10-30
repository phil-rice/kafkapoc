package com.hcltech.rmg.appcontainer.interfaces;

import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.config.configs.Configs;

import java.io.Serializable;

public record AiDefn(RootConfig rootConfig, Configs config, String cel) implements Serializable {
}
