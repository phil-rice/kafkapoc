package com.hcltech.rmg.flinkadapters.context;

import java.util.concurrent.ThreadLocalRandom;

public interface IRandom {
    /**
     * @return a double in [0.0, 1.0)
     */
    double nextDouble();

    static IRandom defaultRandom() {
        return () -> ThreadLocalRandom.current().nextDouble();
    }
}
