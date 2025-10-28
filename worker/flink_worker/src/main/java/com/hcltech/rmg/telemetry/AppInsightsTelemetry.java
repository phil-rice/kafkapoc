package com.hcltech.rmg.telemetry;

import com.microsoft.applicationinsights.TelemetryClient;
import com.microsoft.applicationinsights.attach.ApplicationInsights;
import com.microsoft.applicationinsights.TelemetryConfiguration;

public final class AppInsightsTelemetry {

    private static volatile TelemetryClient CLIENT;

    private AppInsightsTelemetry() {}

    public static synchronized void initGlobal() {
        if (CLIENT != null) return;

        String connStr = System.getenv("APPINSIGHTS_CONNECTION_STRING");
        String ikey    = System.getenv("APPINSIGHTS_INSTRUMENTATIONKEY");

        if (connStr != null && !connStr.isBlank()) {
            TelemetryConfiguration.getActive().setConnectionString(connStr);
        } else if (ikey != null && !ikey.isBlank()) {
            TelemetryConfiguration.getActive().setInstrumentationKey(ikey);
        }

        try {
            ApplicationInsights.attach();
        } catch (Throwable t) {
            System.err.println("AppInsights attach failed: " + t.getMessage());
        }

        CLIENT = new TelemetryClient(TelemetryConfiguration.getActive());
        CLIENT.trackEvent("flink_worker_startup");
        CLIENT.flush();
    }

    public static void trackMetric(String name, double value) {
        TelemetryClient c = CLIENT;
        if (c != null) {
            c.trackMetric(name, value);
        }
    }

    public static void trackException(Throwable t, String where) {
        TelemetryClient c = CLIENT;
        if (c != null) {
            c.trackException(t);
            c.trackEvent("exception@" + where);
        }
    }
}
