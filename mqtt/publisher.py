import json 

import os 

import paho.mqtt.client as mqtt 

 

BROKER = os.getenv("MQTT_BROKER", "localhost") 

PORT = int(os.getenv("MQTT_PORT", "1883")) 

TOPIC = os.getenv("MQTT_TOPIC", "wearable/data") 

 

client = mqtt.Client( 

    mqtt.CallbackAPIVersion.VERSION2 

) 

 

client.connect(BROKER, PORT) 

 

data = { 

    "patient_id": "P002", 

    "heart_rate": 135, 

    "spo2": 89, 

    "temperature": 101.2 

} 

 

message = json.dumps(data) 

 

client.publish(TOPIC, message) 

 

print("Published MQTT message:") 

print(message) 

 

client.disconnect() 