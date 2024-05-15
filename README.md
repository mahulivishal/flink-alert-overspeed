# flink-alert-overspeed
This is a PoC to show how alerts can be generated using a stream of data on certain conditions. Here I take the standard Overspeed Alert used in many Vehicles.


# Steps:
1. **DOWNLOAD AND INSTALL KAFKA**: https://kafka.apache.org/quickstart
2. **RUN ZOOKEEPER**: bin/zookeeper-server-start.sh config/zookeeper.properties
3. **RUN KAFKA**: bin/kafka-server-start.sh config/server.properties
4. **DOWNLOAD FLINK**: https://www.apache.org/dyn/closer.lua/flink/flink-1.19.0/flink-1.19.0-bin-scala_2.12.tgz
5. **EXTRACT CONTENT**: tar -xzf flink-*.tgz
6. **START FLINK**: ./bin/start-cluster.sh
7. **SUBMIT JOB**: ./bin/flink run /Users/vishalmahuli/Desktop/vishal-git/flink-alert-overspeed/target/flink-overspeed-1.0-SNAPSHOT.jar
8. **PUSH SPEED DATA TO SOURCE TOPIC**: bin/kafka-console-producer.sh --topic over.speed.alert.source.v1 --bootstrap-server localhost:9092, RECORD: {"deviceId":"device_01","speedInKmph":10,"timestamp":1715781422000}
9. **LISTEN ON TO SINK TOPIC FOR ALERT**: bin/kafka-console-consumer.sh --topic over.speed.alert.sink.v1 --from-beginning --bootstrap-server localhost:9092
