package vishal.flink.overspeed.alert.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import vishal.flink.overspeed.alert.model.Alert;

@Slf4j
public class SSESink implements SinkFunction<String> {

    private final ObjectMapper mapper = new ObjectMapper();

    private static final OkHttpClient client = new OkHttpClient();
    private final String sseEndpointUrl;

    public SSESink(String sseEndpointUrl) {
        this.sseEndpointUrl = sseEndpointUrl;
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        log.info("SSE Alert: {}", value);
        String endpoint = getDeviceLevelEndpoint(value);
        if(null != endpoint) {
            RequestBody body = RequestBody.create(value, MediaType.parse("text/plain"));
            Request request = new Request.Builder()
                    .url(endpoint)
                    .post(body)
                    .build();
            log.info("SSE URL: {}", endpoint);
            try (Response response = client.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    log.error("API Exception: {}", response);
                }
            }
        }
    }

    private String getDeviceLevelEndpoint(String event){
        try {
            Alert alert = mapper.readValue(event, Alert.class);
            StringBuilder builder = new StringBuilder();
            builder.append(sseEndpointUrl).append(alert.getDeviceId());
            return builder.toString();
        }catch (Exception e){
            log.error("Exception: {}", e);
            return null;
        }
    }
}
