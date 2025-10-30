package sample_api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.embedded.netty.NettyReactiveWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.server.HttpServerRequest;

import java.lang.reflect.Method;

@Configuration
public class SampleApiConfig {

    private static final Logger log = LoggerFactory.getLogger(SampleApiConfig.class);
// in a @Configuration class in a package scanned by @SpringBootApplication

    @Bean
    WebServerFactoryCustomizer<NettyReactiveWebServerFactory> enableH2() {
        return factory -> factory.addServerCustomizers(
                http -> http.protocol(HttpProtocol.H2, HttpProtocol.HTTP11) // enable ALPN H2+H1
        );
    }

    @Bean
    WebFilter logProtocolAndMdc() {
        var log = LoggerFactory.getLogger("sample_api.Protocol");
        return (exchange, chain) -> {
            log.info("start method={} uri={} trace={} span={}",
                    exchange.getRequest().getMethod(),
                    exchange.getRequest().getURI(),
                    org.slf4j.MDC.get("traceId"),
                    org.slf4j.MDC.get("spanId"));
            return chain.filter(exchange)
                    .doOnSuccess(v -> log.info("done  method={} uri={} trace={} span={}",
                            exchange.getRequest().getMethod(),
                            exchange.getRequest().getURI(),
                            org.slf4j.MDC.get("traceId"),
                            org.slf4j.MDC.get("spanId")));
        };
    }

//    @Bean
//    WebServerFactoryCustomizer<NettyReactiveWebServerFactory> nettyCustomizer() {
//        return factory -> factory.addServerCustomizers(http ->
//                http
//                        // Let ALPN negotiate HTTP/2 when the client supports it
//                        .protocol(HttpProtocol.H2, HttpProtocol.HTTP11)
//                        // Enable and customize the access log line
//                        .accessLog(true, args -> {
//                            // W3C header from client (traceparent) and what we echoed back (from your WebFilter)
//                            CharSequence tpIn  = args.requestHeader("traceparent");
//                            CharSequence tpOut = args.responseHeader("traceparent"); // set in your beforeCommit filter
//
//                            // Build a single concise line that includes the protocol and correlation IDs
//                            return String.format(
//                                    "proto=%s method=%s uri=%s status=%s bytes=%s dur=%dµs traceparent_in=%s traceparent_out=%s",
//                                    args.protocol(),            // e.g. HTTP/2.0
//                                    args.method(),               // e.g. GET
//                                    args.uri(),                  // e.g. /sample_api/lookup?q=1
//                                    args.status(),               // e.g. 200
//                                    args.responseHeader("content-length"),
//                                    args.duration(),
//                                    tpIn != null ? tpIn : "-",
//                                    tpOut != null ? tpOut : "-"
//                            );
//                        })
//        );
}
