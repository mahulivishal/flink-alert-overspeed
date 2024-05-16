package vishal.flink.overspeed.alert.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import vishal.flink.overspeed.alert.model.Alert;

@Slf4j
public class SSESink extends RichSinkFunction<String> {

    private transient CloseableHttpClient httpClient;
    private String endpointUrl;

    private final ObjectMapper mapper = new ObjectMapper();

    public SSESink(String endpointUrl) {
        this.endpointUrl = endpointUrl;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        httpClient = HttpClients.createDefault();
    }

    @Override
    public void close() throws Exception {
        super.close();
        httpClient.close();
    }

    @Override
    public void invoke(String event, Context context) throws Exception {
        String deviceLevelEndpoint = getDeviceLevelEndpoint(event);
        if(null != deviceLevelEndpoint) {
            HttpPost httpPost = new HttpPost(deviceLevelEndpoint);
            StringEntity entity = new StringEntity("data: " + event + "\n\n");
            entity.setContentType("text/event-stream");
            entity.setChunked(true);
            httpPost.setEntity(entity);
            log.info("SSE URL: {}", deviceLevelEndpoint);
            log.info("SSE Event: {}", entity.toString());
            httpClient.execute(httpPost);
        }
    }

    private String getDeviceLevelEndpoint(String event){
        try {
            Alert alert = mapper.readValue(event, Alert.class);
            StringBuilder builder = new StringBuilder();
            builder.append(endpointUrl).append(alert.getDeviceId());
            return builder.toString();
        }catch (Exception e){
            log.error("Exception: {}", e);
            return null;
        }
    }
}
