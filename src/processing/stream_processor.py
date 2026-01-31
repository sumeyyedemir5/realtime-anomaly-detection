import json
from kafka import KafkaConsumer
import joblib
import os
import pandas as pd

# 1. Modeli Yükle
model_path = os.path.join('src', 'models', 'anomaly_model.pkl')
model = joblib.load(model_path)

# 2. Kafka Consumer Kurulumu
consumer = KafkaConsumer(
    'sensor-data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='anomaly-detector-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(">>> Canlı Analiz Başladı (Java/Hadoop Gerektirmez)...")

# 3. Gerçek Zamanlı Tahmin Döngüsü
try:
    for message in consumer:
        data = message.value
        
        # Veriyi modelin beklediği formata getir
        features = pd.DataFrame([{
            'temperature': data['temperature'],
            'pressure': data['pressure']
        }])
        
        # Tahmin yap (-1: Anomali, 1: Normal)
        prediction = model.predict(features)[0]
        status = "⚠️ ANOMALİ" if prediction == -1 else "✅ NORMAL"
        
        print(f"[{data['sensor_id']}] Sıcaklık: {data['temperature']:.2f} | Durum: {status}")

except KeyboardInterrupt:
    print(">>> Analiz durduruldu.")