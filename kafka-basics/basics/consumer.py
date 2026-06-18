from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "demo-topic",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

for msg in consumer:
    print(msg.value)