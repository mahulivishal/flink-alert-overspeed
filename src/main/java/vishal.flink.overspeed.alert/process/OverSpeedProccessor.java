package vishal.flink.overspeed.alert.process;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import vishal.flink.overspeed.alert.model.Alert;
import vishal.flink.overspeed.alert.model.OverSpeedAlertState;
import vishal.flink.overspeed.alert.model.SpeedData;

import java.util.ArrayList;

@Slf4j
public class OverSpeedProccessor extends KeyedProcessFunction<String, SpeedData, String> {
    private MapState<String, OverSpeedAlertState> speedState;
    private Long overSpeedThreshold = 100L;

    private Long avgWindowSizeinMS = 10000L;

    public void open(Configuration configuration) throws Exception {
        MapStateDescriptor<String, OverSpeedAlertState> vehicleStateMap = new MapStateDescriptor<>("speed_state_map", String.class, OverSpeedAlertState.class);
        this.speedState = getRuntimeContext().getMapState(vehicleStateMap);
    }
    public void processElement(SpeedData speedData, KeyedProcessFunction<String, SpeedData, String>.Context context, Collector<String> collector) throws Exception {
        try {
            if (speedData.getSpeedInKmph() >= overSpeedThreshold) {
                log.info("Overspeed detected: {}", speedData);
                if (speedState.isEmpty()) {
                    ArrayList<Long> speedValues = new ArrayList<Long>();
                    speedValues.add(speedData.getSpeedInKmph());
                    OverSpeedAlertState overSpeedAlertState = OverSpeedAlertState.builder()
                            .detectionWindowStartTimestamp(speedData.getTimestamp())
                            .speedValues(speedValues)
                            .isAlertSent(true)
                            .build();
                    speedState.put(context.getCurrentKey(), overSpeedAlertState);
                    String message = "OVERSPEEDING at " + speedData.getSpeedInKmph() + "kmph! You've breached the Speed Limit! Please slow down.";
                    log.info("ALERT: {}", message);
                    Alert alert = Alert.builder()
                            .deviceId(context.getCurrentKey())
                            .message(message)
                            .build();
                    collector.collect(alert.toString());
                } else {
                    OverSpeedAlertState overSpeedAlertState = speedState.get(context.getCurrentKey());
                    if (!overSpeedAlertState.getIsAlertSent()) {
                        String message = "OVERSPEEDING at " + speedData.getSpeedInKmph() + "kmph! You've breached the Speed Limit! Please slow down.";
                        log.info("ALERT: {}", message);
                        Alert alert = Alert.builder()
                                .deviceId(context.getCurrentKey())
                                .message(message)
                                .build();
                        collector.collect(alert.toString());
                    }
                }
            } else {
                if (!speedState.isEmpty()) {
                    OverSpeedAlertState overSpeedAlertState = speedState.get(context.getCurrentKey());
                    overSpeedAlertState.setIsAlertSent(false);
                    speedState.put(context.getCurrentKey(), overSpeedAlertState);
                }
            }
        }catch (Exception e){
            log.error("Exception in processor", e);
        }
    }
}
