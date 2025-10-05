package com.hcltech.rmg.flinkadaptersold.FlinkContext;

import com.hcltech.rmg.cepstate.worklease.ITokenGenerator;
import com.hcltech.rmg.common.ITimeService;

public interface FlinkContext {
    ITimeService timeService();

    ITokenGenerator tokenGenerator();

    static FlinkContext byName(String name) {
        if (name.equals("real"))
            return RealFlinkContext.instance;
        throw new IllegalArgumentException("Unknown FlinkContext: " + name + " (known: real)");
    }
}

class RealFlinkContext implements FlinkContext {
    public static final FlinkContext instance = new RealFlinkContext();
    private final ITimeService timeService = ITimeService.real;
    private final ITokenGenerator tokenGenerator = ITokenGenerator.generator();

    @Override
    public ITimeService timeService() {
        return timeService;
    }

    @Override
    public ITokenGenerator tokenGenerator() {
        return tokenGenerator;
    }
}
