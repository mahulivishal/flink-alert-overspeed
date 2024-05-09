package vishal.flink.overspeed.alert.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SpeedData {
    public String deviceId;
    public Long speedInKmph;
    public Long timestamp;
}
