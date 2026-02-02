from datetime import datetime
import json
from kafka import KafkaConsumer
import joblib
import os
import sqlite3
import pandas as pd
from src.models.train import retrain_model

RETRAIN_THRESHOLD = 5000  
last_retrain_count = 0

def check_and_retrain():
    global model, processed_count
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM detection_results")
    row_count = cursor.fetchone()[0]
    conn.close()

    # Eğer veritabanında 1000'den fazla veri varsa ve 
    # son eğitimden bu yana 500 yeni veri birikmişse 
    if row_count >= 1000 and row_count % 500 == 0:
        print(f"\n>>> [MLOps] Yeniden eğitim tetiklendi. Mevcut veri: {row_count}")
        
        retrain_model() 
        
        model = joblib.load(model_path)
        print(">>> [MLOps] Yeni model belleğe yüklendi. Analiz devam ediyor...\n")


#  Veritabanı Kurulumu

DB_PATH = os.path.join('data', 'anomalies.db')
if not os.path.exists('data'):
    os.makedirs('data')

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS detection_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            sensor_id TEXT,
            temperature REAL,
            pressure REAL,
            prediction INTEGER,
            status TEXT
        )
    ''')
    conn.commit()
    conn.close()

init_db()

#  Model Yükleme
model_path = os.path.join('src', 'models', 'anomaly_model.pkl')
model = joblib.load(model_path)

#  Kafka Consumer Kurulumu
consumer = KafkaConsumer(
    'sensor-data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='anomaly-detector-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f">>> Canlı Analiz Başladı , Sonuçlar {DB_PATH} dosyasına kaydediliyor......")
processed_count = 0
#  Gerçek Zamanlı Tahmin Döngüsü
try:
    for message in consumer:
        data = message.value
        processed_count += 1

        # Veriyi modelin beklediği formata getir
        features = pd.DataFrame([{
            'temperature': data['temperature'],
            'pressure': data['pressure']
        }])
        
        # Tahmin yap (-1: Anomali, 1: Normal)
        prediction = model.predict(features)[0]
        status = "⚠️ ANOMALİ" if prediction == -1 else "✅ NORMAL"

        #  Sonuçları Veritabanına Kaydet
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO detection_results (timestamp, sensor_id, temperature, pressure, prediction, status)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
              data['sensor_id'], data['temperature'], data['pressure'], 
              int(prediction), status))
        conn.commit()
        conn.close()

        print(f"[{data['sensor_id']}] Durum: {status} (Veritabanına işlendi)")
        if processed_count % 100 == 0:
            check_and_retrain()
except KeyboardInterrupt:
    print(">>> Analiz durduruldu.")