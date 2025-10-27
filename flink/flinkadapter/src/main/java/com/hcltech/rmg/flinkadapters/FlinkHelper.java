package com.hcltech.rmg.flinkadapters;

import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class FlinkHelper {

    public static Configuration makeDefaultFlinkConfig() {
        Configuration conf = new Configuration();

// Web UI fixed off 8081
        conf.setString("rest.address", "localhost");
        conf.setString("rest.port", "8088");
        System.out.println("Flink Web UI at http://localhost:8088");
        conf.setString("metrics.reporter.promjm.factory.class", "org.apache.flink.metrics.prometheus.PrometheusReporterFactory");
        conf.setString("metrics.reporter.promjm.port", "9400");

        conf.setString("jobmanager.metrics.reporters", "promjm");
        conf.setString("taskmanager.metrics.reporters", "promtm");
        return conf;
    }

    /**
     * Probe http://localhost:[from..to]/metrics and print the first that responds.
     */
    public static void probeMetricsPort(int fromInclusive, int toInclusive) {
        // Give the MiniCluster a moment to boot reporters
        try {
            Thread.sleep(1500);
        } catch (InterruptedException ignored) {
        }
        for (int p = fromInclusive; p <= toInclusive; p++) {
            try {
                URL u = new URL("http://localhost:" + p + "/metrics");
                HttpURLConnection c = (HttpURLConnection) u.openConnection();
                c.setConnectTimeout(400);
                c.setReadTimeout(400);
                c.setRequestMethod("GET");
                int code = c.getResponseCode();
                if (code == 200) {
                    System.out.println("[metrics] Prometheus endpoint: http://localhost:" + p + "/metrics");
                    return;
                }
            } catch (IOException ignored) { /* try next */ }
        }
        System.out.println("[metrics] No /metrics endpoint found in " + fromInclusive + "-" + toInclusive);
    }
}
