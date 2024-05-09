FROM flink:1.15.4-scala_2.12-java11

RUN mkdir /opt/flink/plugins/flink-s3-fs-hadoop

COPY ./flink-s3-fs-hadoop-1.15.4.jar /opt/flink/plugins/flink-s3-fs-hadoop/flink-s3-fs-hadoop-1.15.4.jar

RUN chmod +x /opt/flink/plugins/flink-s3-fs-hadoop/flink-s3-fs-hadoop-1.15.4.jar

RUN mkdir /flink-jar

COPY ./target/datalake-flink-*.jar /flink-jar/app.jar

