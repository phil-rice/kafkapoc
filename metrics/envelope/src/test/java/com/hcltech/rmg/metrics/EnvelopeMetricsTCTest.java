package com.hcltech.rmg.metrics;

import com.hcltech.rmg.messages.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EnvelopeMetricsTCTest {

    RawMessage rawMessage;

    {
        long processingTimestamp = 2000L;
        rawMessage = new RawMessage("raw", "domId", 1000L, processingTimestamp, 10, 1000, "traceParent", "traceState", "baggage");
    }

    EnvelopeHeader<String> header = new EnvelopeHeader<>("domType", "evType", rawMessage,null, null);
    ValueEnvelope<String, String> valueEnvelope=new ValueEnvelope<>(header, "data", null, List.of());
    RetryEnvelope<String, String> retryEnvelope=new RetryEnvelope<>(valueEnvelope, "stage", 1);
    ErrorEnvelope<String, String> errorEnvelope=new ErrorEnvelope<>(valueEnvelope, "errorMsg", List.of("some message"));
    EnvelopeMetricsTC sut = new EnvelopeMetricsTC();

    @Test
    void metricName_valueEnvelope_success() {
        String metricName = sut.metricName(valueEnvelope);
        assertEquals("success", metricName);
    }

    @Test
    void metricName_retryEnvelope_retry() {
        String metricName = sut.metricName(retryEnvelope);
        assertEquals("retry", metricName);
    }
    @Test
    void metricName_errorEnvelope_error() {
        String metricName = sut.metricName(errorEnvelope);
        assertEquals("error", metricName);
    }

    @Test
    void valueEnvelope_startProcessingTime() {
        assertEquals(2000L, sut.startProcessingTime(valueEnvelope));
    }

    @Test
    void retryEnvelope_startProcessingTime() {
        assertEquals(2000L, sut.startProcessingTime(retryEnvelope));
    }

    @Test
    void errorEnvelope_startProcessingTime() {
        assertEquals(2000L, sut.startProcessingTime(errorEnvelope));
    }
}