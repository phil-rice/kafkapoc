package sample_api;

import io.micrometer.observation.annotation.Observed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.Map;

@RestController
@RequestMapping(path = "/sample_api", produces = MediaType.APPLICATION_JSON_VALUE)
public class LookupController {
    private static final Logger log = LoggerFactory.getLogger(LookupController.class);

    @Observed(name = "http.server.lookup", contextualName = "lookup")
    @GetMapping("/lookup")
    public Mono<Map<String, Object>> lookup(@RequestParam("q") String q) {
        return Mono.just(Map.<String,Object>of("sample", "value"))
                .doOnSuccess(v -> log.info("done /lookup"));
    }
}
