import json
import random
import time

from pykafka import KafkaClient

kafka_brokers = "localhost:9092"
speed_data_source_data = "over.speed.alert.source.sse.v1"
speed_data_sink_data = "over.speed.alert.sink.v1"
client = KafkaClient(hosts=kafka_brokers)
topic = client.topics[speed_data_source_data]
producer = topic.get_sync_producer()
speed_threshold = 100
speed_random_neg_margin = 50
speed_random_pos_margin = 10
no_of_packets = 10
devices = ["d_01", "d_02", "d_03"]


def push_speed_data(data):
    message = json.dumps(data)
    message_bytes = message.encode('utf-8')
    producer.produce(message_bytes)


def prepare_data():
    i = 0
    while i < no_of_packets:
        data = {"deviceId": random.choice(devices), "timestamp": int(time.time() * 1000),
                "speedInKmph": random.randint(speed_threshold - speed_random_neg_margin,
                                              speed_threshold + speed_random_pos_margin)}
        print(data)
        time.sleep(0.5)
        push_speed_data(data)
        i += 1


if __name__ == '__main__':
    prepare_data()
