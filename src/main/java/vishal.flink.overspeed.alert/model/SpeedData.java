package vishal.flink.overspeed.alert.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SpeedData {
    public String deviceId;
    public Long speedInKmph;
    public Long timestamp;
}
