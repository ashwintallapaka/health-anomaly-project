import json
import os

from kafka import KafkaProducer

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "health-data")

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

data = {
    "patient_id": "P001",
    "heart_rate": 75,
    "spo2": 98,
    "temperature": 98.6
}

future = producer.send(TOPIC, value=data)
metadata = future.get(timeout=10)

print("Published Kafka message:")
print(data)

print(
    f"Topic={metadata.topic}, "
    f"Partition={metadata.partition}, "
    f"Offset={metadata.offset}"
)

producer.flush()
producer.close()
