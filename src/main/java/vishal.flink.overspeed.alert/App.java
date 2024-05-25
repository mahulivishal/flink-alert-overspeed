package vishal.flink.overspeed.alert;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
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
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/g0006686/Desktop/checkpoints");
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(2000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // Setting state backend with incremental checkpointing enabled
        process(env);
        env.execute("Vishal Flink Alert Overspeed");
    }

    private static void process(StreamExecutionEnvironment env) throws Exception {
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", "localhost:9092");
        consumerProps.setProperty("group.id", "test");
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "localhost:9092");
        producerProps.setProperty("acks", "1");

        FlinkKafkaConsumer<String> speedDataSource = new FlinkKafkaConsumer<String>("over.speed.alert.source.v1",
                new SimpleStringSchema(), consumerProps);
        DataStream<String> speedDataStream = env.addSource(speedDataSource).setParallelism(1).name("speed-data-source")
                .map(new SpeedDataMapper()).setParallelism(1).name("data-mapper")
                .filter(new NullFilters<SpeedData>()).setParallelism(1).name("null-filter")
                .keyBy(SpeedData::getDeviceId)
                .process(new OverSpeedProccessor()).setParallelism(1).name("over-speed-processor");
        speedDataStream.addSink(new FlinkKafkaProducer<>("over.speed.alert.sink.v1", new SimpleStringSchema(), producerProps)).setParallelism(1).name("alert-sink");
    }

    }
