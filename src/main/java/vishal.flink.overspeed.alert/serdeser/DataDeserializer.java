package vishal.flink.overspeed.alert.serdeser;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Generic Deserializer for any POJO
 */
@Slf4j
@NoArgsConstructor
public class DataDeserializer<T> implements DeserializationSchema<T> {
    private static final ObjectMapper mapper = new ObjectMapper();

    TypeInformation<T> typeInformation;
    public DataDeserializer(TypeInformation<T> type) {
        this.typeInformation = type;
    }

    public T deserialize(byte[] bytes) throws IOException {
        try {
            T tData = mapper.readValue(bytes, typeInformation.getTypeClass());
            return tData;
        } catch (Exception e) {
            log.error("Error while trying to deserialize event " + typeInformation, e);
        }
        return null;
    }

    public boolean isEndOfStream(T tData) {
        return false;
    }

    public TypeInformation<T> getProducedType() {
        return typeInformation;
    }
}
