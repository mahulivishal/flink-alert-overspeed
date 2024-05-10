package vishal.flink.overspeed.alert.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OverSpeedAlertState {
    public Long detectionWindowStartTimestamp;
    public ArrayList<Long> speedValues;
}
