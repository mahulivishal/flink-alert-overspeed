package vishal.sse.server.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.MediaType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@RestController
@RequestMapping("/overspeed-alert")  // Base path for this controller
@Slf4j
public class SSEController {

    private final Sinks.Many<String> sink;

    public SSEController() {
        this.sink = Sinks.many().multicast().onBackpressureBuffer();
    }

    @GetMapping(value = "/sse/d_01", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> streamEvents() {
        log.info("Sending Data!");
        return sink.asFlux().map(data -> "data:" + data + "\n\n");
    }

    @PostMapping(value = "/sse/d_01", consumes = "text/plain")
    public void pushEvent(@RequestBody String data) {
        log.info("Data Received: {}", data);
        sink.tryEmitNext(data);
    }
}