import json
from kafka import KafkaConsumer
import pandas as pd
import os

# 1. Klasörü kontrol et
if not os.path.exists('data'):
    os.makedirs('data')

# 2. Kafka Consumer kurulumu
consumer = KafkaConsumer(
    'sensor-data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

data_list = []
print(">>> Veri toplanıyor... (Durdurmak için Ctrl+C)")

try:
    for message in consumer:
        data_list.append(message.value)
        # Her 100 veride bir dosyayı güncelle
        if len(data_list) % 100 == 0:
            df = pd.DataFrame(data_list)
            df.to_csv('data/raw_sensor_data.csv', index=False)
            print(f">>> {len(data_list)} veri kaydedildi.")
except KeyboardInterrupt:
    # Programı kapattığında son kalanları da kaydet
    df = pd.DataFrame(data_list)
    df.to_csv('data/raw_sensor_data.csv', index=False)
    print(">>> Toplama işlemi tamamlandı.")