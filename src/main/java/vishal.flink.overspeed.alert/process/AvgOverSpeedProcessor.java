package vishal.flink.overspeed.alert.process;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import vishal.flink.overspeed.alert.model.Alert;
import vishal.flink.overspeed.alert.model.SpeedData;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class AvgOverSpeedProcessor extends ProcessAllWindowFunction<SpeedData, String, TimeWindow> {

    private final ObjectMapper mapper = new ObjectMapper();

    private MapState<String, Long> speedState;

    private Long overSpeedThreshold = 100L;

    private Long alertTimeDiffThresoldInMS = 60000L;

    @Override
    public void process(Context context, Iterable<SpeedData> iterable, Collector<String> collector) throws Exception {
        List<Long> speedValues = new ArrayList<>();
        String deviceId = null;
        long windowLatestEventTimeStamp = 0L;
        for(SpeedData speedData: iterable) {
            speedValues.add(speedData.getSpeedInKmph());
            deviceId = speedData.getDeviceId();
            if(windowLatestEventTimeStamp < speedData.getTimestamp())
                windowLatestEventTimeStamp = speedData.getTimestamp();
        }
        if(!speedValues.isEmpty()) {
            double avg = speedValues.stream()
                    .mapToLong(l -> l)
                    .average()
                    .orElse(0L);
            if (avg > overSpeedThreshold) {
                if (null != speedState) {
                    Long lastAlertTimeStamp = speedState.get(deviceId);
                    if (windowLatestEventTimeStamp - lastAlertTimeStamp < alertTimeDiffThresoldInMS)
                        return;
                }
                String message = "OVERSPEEDING at " + avg + "kmph! You've breached the Speed Limit! Please slow down.";
                log.info("ALERT: {}", message);
                Alert alert = Alert.builder()
                        .deviceId(deviceId)
                        .message(message)
                        .build();
                collector.collect(mapper.writeValueAsString(alert));
                speedState.put(deviceId, windowLatestEventTimeStamp);
            }
        }

    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
    }
}
