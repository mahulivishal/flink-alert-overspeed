package vishal.flink.overspeed.alert.filters;

import org.apache.flink.api.common.functions.FilterFunction;
import vishal.flink.overspeed.alert.model.SpeedData;

public class OverSpeedFilter implements FilterFunction<SpeedData> {

    private Long overSpeedThreshold = 0L;

    private Long OutOfOrderTimeThresholdInMS = 60000L;

    @Override
    public boolean filter(SpeedData speedData) throws Exception {
        return ((System.currentTimeMillis() - speedData.getTimestamp() <= OutOfOrderTimeThresholdInMS)
                && (speedData.getSpeedInKmph() > overSpeedThreshold));
    }
}
