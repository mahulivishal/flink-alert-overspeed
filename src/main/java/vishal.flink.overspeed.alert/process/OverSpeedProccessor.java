package vishal.flink.overspeed.alert.process;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import vishal.flink.overspeed.alert.model.OverSpeedAlertState;
import vishal.flink.overspeed.alert.model.SpeedData;
import vishal.flink.overspeed.alert.model.VehicleState;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class OverSpeedProccessor extends KeyedProcessFunction<String, SpeedData, String> {
    private MapState<String, OverSpeedAlertState> speedTimestampState;
    private ValueState<VehicleState> vehicleStateValueState;
    private Long overSpeedThreshold = 100L;

    private Long avgWindowSizeinMS = 10000L;


    public void processElement(SpeedData speedData, KeyedProcessFunction<String, SpeedData, String>.Context context, Collector<String> collector) throws Exception {
        VehicleState vehicleState = vehicleStateValueState.value();
        if(null == vehicleState){
            vehicleState.setVin(context.getCurrentKey());
            vehicleState.setOverSpeedTheshold(overSpeedThreshold);
            vehicleStateValueState.update(vehicleState);
        }
        if(speedData.getSpeedInKmph() >= overSpeedThreshold){
            if(speedTimestampState.isEmpty()) {
                ArrayList<Long> speedValues = new ArrayList<Long>();
                speedValues.add(speedData.getSpeedInKmph());
                OverSpeedAlertState overSpeedAlertState = OverSpeedAlertState.builder()
                        .detectionWindowStartTimestamp(speedData.getTimestamp())
                        .speedValues(speedValues)
                        .build();
                speedTimestampState.put(context.getCurrentKey(), overSpeedAlertState);
            } else {
                OverSpeedAlertState overSpeedAlertState = speedTimestampState.get(context.getCurrentKey());
                if(speedData.getTimestamp() - overSpeedAlertState.getDetectionWindowStartTimestamp() >= avgWindowSizeinMS){
                    List<Long> speedValues = overSpeedAlertState.getSpeedValues();
                    speedValues.add(speedData.getSpeedInKmph());
                    double avg = speedValues.stream().mapToLong(Long::longValue).average().orElse(0);
                    if(avg >= overSpeedThreshold){
                        speedTimestampState.remove(context.getCurrentKey());
                        String message = context.getCurrentKey() + ": OVERSPEEDING! You've breached the Speed Limit! Please slow down.";
                        collector.collect(message);
                    }
                }else{
                    overSpeedAlertState.getSpeedValues().add(speedData.getSpeedInKmph());
                    speedTimestampState.put(context.getCurrentKey(), overSpeedAlertState);
                }
            }
        }
    }
}
