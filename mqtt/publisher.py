import json 

import os 

import paho.mqtt.client as mqtt 

import time

import random

from datetime import datetime 

BROKER = os.getenv("MQTT_BROKER", "localhost") 

PORT = int(os.getenv("MQTT_PORT", "1883")) 

TOPIC = os.getenv("MQTT_TOPIC", "wearable/data") 

 

client = mqtt.Client( 

    mqtt.CallbackAPIVersion.VERSION2 

) 

 

client.connect(BROKER, PORT) 
client.loop_start()

normal = {"heart_rate": 75, "spo2": 98, "temperature": 98.6}
abnormal = {"heart_rate": 135, "spo2": 89, "temperature": 101.2}


while True:
    reading = abnormal if random.random() < 0.2 else normal
    
    data = {
        "patient_id": random.choice(["P001", "P002", "P003"]),

        "heart_rate": reading["heart_rate"],

        "spo2": reading["spo2"],

        "temperature": reading["temperature"],
        
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }

    message = json.dumps(data) 
    client.publish(TOPIC, message) 
    print("Published MQTT message:") 
    print(message)
    time.sleep(2) 
    

 
