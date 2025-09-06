package com.hcltech.rmg.flinkadapters.envelopes;

import java.io.Serializable;

public interface ValueRetryErrorEnvelope extends Serializable {
    String domainId();
}
