package vishal.flink.overspeed.alert.serdeser;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import vishal.flink.overspeed.alert.model.KafkaRecord;

import javax.annotation.Nullable;

public class KafkaRecordSerializer implements KafkaSerializationSchema<KafkaRecord> {

    private String topicName;

    public KafkaRecordSerializer(String topicName) {
        super();
        this.topicName = topicName;
    }

    public ProducerRecord<byte[], byte[]> serialize(KafkaRecord kafkaRecord, @Nullable Long aLong) {
        return new ProducerRecord<byte[], byte[]>(topicName, kafkaRecord.getKey().getBytes(), kafkaRecord.getValue().getBytes());
    }
}
