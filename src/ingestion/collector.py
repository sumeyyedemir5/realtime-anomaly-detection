import json
import pandas as pd
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'sensor-data',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

data_list = []
print("Starting sensor data collection...")
try:
     for message in consumer:
          data_list.append(message.value)
          if len(data_list) % 10 == 0:
               print(f"Collected {len(data_list)} messages")

          if len(data_list) >= 100:
               df = pd.DataFrame(data_list)
               df.to_csv('../../data/raw_sensor_data.csv', index =False)
               print("Saved 100 messages to raw_sensor_data.csv")
               break
except KeyboardInterrupt:
     print("Stopping sensor data collection...")          