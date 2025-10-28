package com.hcltech.rmg.telemetry;

import com.microsoft.applicationinsights.TelemetryClient;
import com.microsoft.applicationinsights.attach.ApplicationInsights;

/**
 * Works with Application Insights 3.x.
 * No direct TelemetryConfiguration access.
 */
public final class AppInsightsTelemetry {

    private static volatile TelemetryClient client;

    private AppInsightsTelemetry() {}

    public static synchronized void initGlobal() {
        if (client != null) return;

        try {
            // Automatically attaches agent; reads connection string from env
            ApplicationInsights.attach();
        } catch (Throwable t) {
            System.err.println("Application Insights attach failed: " + t.getMessage());
        }

        client = new TelemetryClient();
        client.trackEvent("flink_worker_startup");
        client.flush();
    }

    public static void trackMetric(String name, double value) {
        if (client != null) client.trackMetric(name, value);
    }

    public static void trackException(Exception ex, String where) {
        if (client != null) {
            client.trackException(ex);
            client.trackEvent("exception@" + where);
        }
    }
}
