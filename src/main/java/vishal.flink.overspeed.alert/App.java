package vishal.flink.overspeed.alert;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vishal.flink.overspeed.alert.filters.NullFilters;
import vishal.flink.overspeed.alert.map.SpeedDataMapper;
import vishal.flink.overspeed.alert.model.SpeedData;
import vishal.flink.overspeed.alert.process.OverSpeedProccessor;

import java.util.Properties;

public class App {
    private static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/vishalmahuli/Desktop/checkpoints");
        env.enableCheckpointing(60000L);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(2000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // Setting state backend with incremental checkpointing enabled
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/shivam-emotorad/Downloads/checkpoints");
        process(env);
        env.execute("Vishal Flink Alert Overspeed");
    }

    private static void process(StreamExecutionEnvironment env) throws Exception {
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", "localhost:9092");
        consumerProps.setProperty("zookeeper.connect", "localhost:2181");
        consumerProps.setProperty("group.id", "test");
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaConsumer09<String> speedDataSource = new FlinkKafkaConsumer09<String>("over.speed.alert.source.v1",
                new SimpleStringSchema(), consumerProps);
        DataStream<String> speedDataStream = env.addSource(speedDataSource).setParallelism(1).name("speed-data-source")
                .filter(new NullFilters<String>()).setParallelism(1).name("null-filter")
                .map(new SpeedDataMapper()).setParallelism(1).name("data-mapper")
                .keyBy(SpeedData::getDeviceId)
                .process(new OverSpeedProccessor()).setParallelism(1).name("over-speed-processor");
        speedDataStream.addSink(new FlinkKafkaProducer<>("over.speed.alert.sink.v1", new SimpleStringSchema(), producerProps));
       /* speedDataStream.addSink(new FlinkKafkaProducer<KafkaRecord>("over.speed.alert.sink.v1",
                new KafkaRecordSerializer("over.speed.alert.sink.v1"), producerProps, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
                .setParallelism(1).name("over-speed-sink");*/
    }

    }
