package vishal.flink.overspeed.alert.map;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import vishal.flink.overspeed.alert.model.SpeedData;

@Slf4j
public class SpeedDataMapper implements MapFunction<String, SpeedData> {
    private final ObjectMapper mapper = new ObjectMapper();

    public SpeedData map(String event) throws Exception {
        try {
            SpeedData speedData = mapper.readValue(event, SpeedData.class);
            log.info("speedData: {}", speedData);
            return speedData;
        }catch (Exception e){
            log.error("speedData ERROR: {}", event);
            log.error("Error: ", e);
            return null;
        }
    }
}
