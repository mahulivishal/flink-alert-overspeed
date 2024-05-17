package vishal.sse.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SSEServer {
    private static Logger logger = LoggerFactory.getLogger(SSEServer.class);
    public static void main(String[] args) {
        SpringApplication.run(SSEServer.class, args);
    }
}
