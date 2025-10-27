package sample_api;

import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Hooks;

@Configuration
class ReactorContextPropagationConfig {
  @PostConstruct
  void enableMicrometerContext() {
    // Copies Micrometer’s tracing + MDC context across reactive operators/threads
    Hooks.enableAutomaticContextPropagation();
  }
}
