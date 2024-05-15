import json
import random
import time

from pykafka import KafkaClient

kafka_brokers = "localhost:9092"
speed_data_source_data = "over.speed.alert.source.v1"
speed_data_sink_data = "over.speed.alert.sink.v1"
client = KafkaClient(hosts=kafka_brokers)
topic = client.topics[speed_data_source_data]
speed_threshold = 100
speed_random_margin = 20
no_of_packets = 10
devices = ["d_01", "d_02", "d_03"]


def push_speed_data(data):
    with topic.get_sync_producer() as producer:
        message = json.dumps(data)
        message_bytes = message.encode('utf-8')
        producer.produce(message_bytes)


def prepare_data():
    i = 0
    while i < no_of_packets:
        data = {"deviceId": random.choice(devices), "timestamp": (time.time() * 1000),
                "speedInKmph": random.randint(speed_threshold - speed_random_margin,
                                              speed_threshold + speed_random_margin)}
        print(data)
        push_speed_data(data)
        i += 1


if __name__ == '__main__':
    prepare_data()
