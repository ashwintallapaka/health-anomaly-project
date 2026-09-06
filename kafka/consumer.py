import json
import os

from kafka import KafkaConsumer

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "health-data")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKER,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="health-anomaly-python-consumer",
    value_deserializer=lambda message: json.loads(
        message.decode("utf-8")
    )
)

print("Connected to Kafka")
print("Waiting for health data...")

for message in consumer:
    print("\nReceived Kafka message:")
    print(message.value)

    print(
        f"Topic={message.topic}, "
        f"Partition={message.partition}, "
        f"Offset={message.offset}"
    )
