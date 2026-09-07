"""
MQTT -> Kafka bridge.
Subscribes to the wearable MQTT topic and forwards every message
to the Kafka 'health-data' topic, unchanged.
"""

import json
import paho.mqtt.client as mqtt
from kafka import KafkaProducer

# --- Config ---
MQTT_BROKER_HOST = "localhost"      # change if MQTT runs on a different host
MQTT_BROKER_PORT = 1883
MQTT_TOPIC = "wearable/data"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"  # change if Kafka runs elsewhere
KAFKA_TOPIC = "health-data"

# --- Kafka producer setup ---
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Connected to MQTT broker")
        client.subscribe(MQTT_TOPIC)
        print(f"Subscribed to: {MQTT_TOPIC}")
    else:
        print(f"Failed to connect to MQTT broker, return code {rc}")


def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode("utf-8")
        print(f"Received from MQTT:\n{payload_str}")

        # Parse as JSON if possible, otherwise forward raw string
        try:
            payload = json.loads(payload_str)
        except json.JSONDecodeError:
            payload = {"raw": payload_str}

        future = producer.send(KAFKA_TOPIC, value=payload)
        result = future.get(timeout=10)  # block briefly to get partition/offset

        print(
            f"Forwarded to Kafka:\n"
            f"Topic={result.topic}, Partition={result.partition}, Offset={result.offset}\n"
        )

    except Exception as e:
        print(f"Error forwarding message: {e}")


def main():
    print("MQTT -> Kafka bridge running...")
    print(f"{MQTT_TOPIC} -> {KAFKA_TOPIC}")

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, keepalive=60)
    client.loop_forever()


if __name__ == "__main__":
    main()
