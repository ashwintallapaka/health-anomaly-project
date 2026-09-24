import json
import os
import random
import time
from datetime import datetime

import paho.mqtt.client as mqtt

BROKER = os.getenv("MQTT_BROKER", "localhost")
PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC = os.getenv("MQTT_TOPIC", "wearable/data")

# Share of readings that are deliberately abnormal (was 0.2).
ANOMALY_RATE = float(os.getenv("ANOMALY_RATE", "0.05"))

# Each simulated user has their own resting heart rate, so the batch job's
# per-user baselines (mean / std) have something real to describe.
USERS = {
    "P001": {"hr": 72, "temp": 36.8, "spo2": 97},
    "P002": {"hr": 65, "temp": 36.6, "spo2": 98},
    "P003": {"hr": 80, "temp": 36.9, "spo2": 96},
}


def normal_reading(base):
    """A plausible resting reading: small random variation around the user's own baseline."""
    return {
        "heart_rate": int(min(115, max(55, random.gauss(base["hr"], 4)))),  # e.g. 72 -> 66..78, never past a rule
        "body_temp_c": round(random.gauss(base["temp"], 0.15), 1),  # e.g. 36.8 -> 36.5..37.1
        "spo2": int(min(99, max(94, random.gauss(base["spo2"], 1)))),
    }


def abnormal_reading(base):
    """Start from a normal reading, then make exactly one vital abnormal.

    Three kinds break a fixed threshold (heart_rate / spo2 / body_temp_c).
    The fourth, "personal", stays INSIDE the global thresholds but is far from
    this user's own baseline, so only the batch z-score rule can catch it.
    """
    r = normal_reading(base)
    rule = random.choice(["heart_rate", "spo2", "body_temp_c", "personal"])
    if rule == "heart_rate":
        r["heart_rate"] = random.choice([random.randint(38, 49), random.randint(125, 155)])
    elif rule == "spo2":
        r["spo2"] = random.randint(84, 91)
    elif rule == "body_temp_c":
        r["body_temp_c"] = round(random.uniform(38.2, 39.6), 1)
    else:
        # e.g. P002 (baseline 65) -> 95..105, P003 (baseline 80) -> 110..119:
        # normal by the fixed rules, 4-5 standard deviations off for that person
        shift = random.choice([-1, 1]) * random.randint(30, 40)
        r["heart_rate"] = int(min(119, max(51, base["hr"] + shift)))
    return r


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect(BROKER, PORT)
client.loop_start()

while True:
    user_id = random.choice(list(USERS))
    is_anomaly = random.random() < ANOMALY_RATE
    reading = abnormal_reading(USERS[user_id]) if is_anomaly else normal_reading(USERS[user_id])

    data = {
        "user_id": user_id,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "heart_rate": reading["heart_rate"],
        "body_temp_c": reading["body_temp_c"],
        "spo2": reading["spo2"],
        "is_injected_anomaly": is_anomaly,
    }

    message = json.dumps(data)
    client.publish(TOPIC, message)
    print("Published MQTT message:")
    print(message)
    time.sleep(2)
