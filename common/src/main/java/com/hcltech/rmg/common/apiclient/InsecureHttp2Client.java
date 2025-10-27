package com.hcltech.rmg.common.apiclient;

import javax.net.ssl.*;
import java.net.http.HttpClient;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;

public class InsecureHttp2Client {
    public static HttpClient insecureHttp2Client(HttpClientConfig<?,?> config) {
        try {
            TrustManager[] trustAll = new TrustManager[]{
                    new X509TrustManager() {
                        public void checkClientTrusted(X509Certificate[] c, String a) {
                        }

                        public void checkServerTrusted(X509Certificate[] c, String a) {
                        }

                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }
                    }
            };
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, trustAll, new SecureRandom());

            // Disable HTTPS hostname verification if your self-signed cert doesn’t have SAN=localhost
            javax.net.ssl.SSLParameters params = new javax.net.ssl.SSLParameters();
            params.setEndpointIdentificationAlgorithm(null);

            return HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_2) // ALPN on JDK 11+ will negotiate h2 with Boot’s Netty
                    .connectTimeout(config.connectTimeout())
                    .sslContext(ctx)
                    .sslParameters(params)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
