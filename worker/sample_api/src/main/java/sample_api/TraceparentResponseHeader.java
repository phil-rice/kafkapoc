package sample_api;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.server.WebFilter;
import reactor.core.publisher.Mono;

@Configuration
class TraceparentResponseHeader {

    @Bean
    public WebFilter addTraceparentHeader(Tracer tracer) {
        return (exchange, chain) -> chain.filter(exchange)
                .then(Mono.defer(() -> {
                    Span span = tracer.currentSpan();
                    if (span != null) {
                        TraceContext ctx = span.context();
                        String flags = ctx.sampled() ? "01" : "00";
                        String tp = "00-" + ctx.traceId() + "-" + ctx.spanId() + "-" + flags;
                        // ensure header is set before the response is committed
                        exchange.getResponse().beforeCommit(() -> {
                            exchange.getResponse().getHeaders().set("traceparent", tp);
                            return Mono.empty();
                        });
                    }
                    return Mono.empty();
                }));
    }
}
