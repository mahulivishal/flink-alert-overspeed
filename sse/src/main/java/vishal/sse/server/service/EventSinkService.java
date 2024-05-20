package vishal.sse.server.service;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Sinks;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
public class EventSinkService {

    private final ConcurrentMap<String, Sinks.Many<String>> sinkMap = new ConcurrentHashMap<>();

    public Sinks.Many<String> createSink(String id) {
        Sinks.Many<String> sink = Sinks.many().unicast().onBackpressureBuffer();
        sinkMap.put(id, sink);
        return sink;
    }

    public Sinks.Many<String> getSink(String id) {
        return sinkMap.get(id);
    }

    public void removeSink(String id) {
        sinkMap.remove(id);
    }
}
