import os 

import paho.mqtt.client as mqtt 

 

BROKER = os.getenv("MQTT_BROKER", "localhost") 

PORT = int(os.getenv("MQTT_PORT", "1883")) 

TOPIC = os.getenv("MQTT_TOPIC", "wearable/data") 

 

 

def on_connect(client, userdata, flags, reason_code, properties): 

    print("Connected to MQTT broker") 

    print("Subscribed to:", TOPIC) 

 

    client.subscribe(TOPIC) 

 

 

def on_message(client, userdata, message): 

    print("\nReceived MQTT message:") 

    print(message.payload.decode()) 

 

 

client = mqtt.Client( 

    mqtt.CallbackAPIVersion.VERSION2 

) 

 

client.on_connect = on_connect 

client.on_message = on_message 

 

client.connect(BROKER, PORT) 

 

print("Waiting for wearable data...") 

 

client.loop_forever() 