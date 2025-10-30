package com.hcltech.rmg.metrics;

import com.hcltech.rmg.common.metrics.IEnvelopeMetricsTC;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.ErrorEnvelope;
import com.hcltech.rmg.messages.RetryEnvelope;
import com.hcltech.rmg.messages.ValueEnvelope;

public class EnvelopeMetricsTC implements IEnvelopeMetricsTC<Envelope<?, ?>> {
    public static final EnvelopeMetricsTC INSTANCE = new EnvelopeMetricsTC();
    private EnvelopeMetricsTC() {
    }
    @Override
    public String metricName(Envelope<?, ?> envelope) {
        if (envelope instanceof ValueEnvelope<?, ?>) {
            return "success";
        } else if (envelope instanceof RetryEnvelope<?, ?>) {
            return "retry";
        } else if (envelope instanceof ErrorEnvelope<?, ?>) {
            return "error";
        } else {
            return "unknown";
        }
    }

    @Override
    public long startProcessingTime(Envelope<?, ?> envelope) {
        return envelope.valueEnvelope().header().rawMessage().processingTimestamp();
    }
}
