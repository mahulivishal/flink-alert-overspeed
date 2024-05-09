package vishal.flink.overspeed.alert.process;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import vishal.flink.overspeed.alert.model.SpeedData;
import vishal.flink.overspeed.alert.model.VehicleState;

import java.util.HashMap;

@Slf4j
public class OverSpeedProccessor extends KeyedProcessFunction<String, SpeedData, HashMap<String, Object>> {
    private MapState<Long, Long> speedTimestampState;
    private ValueState<VehicleState> vehicleStateValueState;
    private Long overSpeedThreshold = 100L;

    public void processElement(SpeedData speedData, KeyedProcessFunction<String, SpeedData, HashMap<String, Object>>.Context context, Collector<HashMap<String, Object>> collector) throws Exception {
        VehicleState vehicleState = vehicleStateValueState.value();
        if(null == vehicleState){
            vehicleState.setVin(context.getCurrentKey());
            vehicleState.setOverSpeedTheshold(overSpeedThreshold);
            vehicleStateValueState.update(vehicleState);
        }
        if(speedData.getSpeedInKmph() >= overSpeedThreshold){
            speedTimestampState.put(System.currentTimeMillis(), speedData.getSpeedInKmph());
        }
    }
}
