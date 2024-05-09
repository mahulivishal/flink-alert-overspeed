package vishal.flink.overspeed.alert.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class VehicleState {
    public String vin;
    public Long overSpeedTheshold;
}

