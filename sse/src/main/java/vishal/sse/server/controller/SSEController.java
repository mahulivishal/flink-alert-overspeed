package vishal.sse.server.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.MediaType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import vishal.sse.server.service.EventSinkService;

@RestController
@RequestMapping("/overspeed-alert")  // Base path for this controller
@Slf4j
public class SSEController {

    private final EventSinkService eventSinkService;

    public SSEController(EventSinkService eventSinkService) {
        this.eventSinkService = eventSinkService;
    }

    @GetMapping(value = "/sse/{id}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> streamEvents(@PathVariable String id) {
        Sinks.Many<String> sink = eventSinkService.getSink(id);
        if (sink == null) {
            sink = eventSinkService.createSink(id);
        }
        log.info("Pushing Data to client: {}", id);
        return sink.asFlux().map(data -> "data:" + data + "\n\n");
    }

    @PostMapping(value = "/sse/{id}")
    public void pushEvent(@PathVariable String id, @RequestBody String data) {
        Sinks.Many<String> sink = eventSinkService.getSink(id);
        if (sink != null) {
            sink.tryEmitNext(data);
        }
    }
}