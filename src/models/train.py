import sqlite3
import pandas as pd
from sklearn.ensemble import IsolationForest
import joblib
import os

def retrain_model():
    # 1. Güncel veriyi SQLite'den çek
    DB_PATH = os.path.join('data', 'anomalies.db')
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT temperature, pressure FROM detection_results", conn)
    conn.close()

    if len(df) <1000: return  # Yeterli veri yoksa çık

    # 2. Modeli yeniden eğit
    new_model = IsolationForest(contamination=0.05, random_state=42)
    new_model.fit(df[['temperature', 'pressure']])

    # 3. Yeni modeli kaydet
    joblib.dump(new_model, os.path.join('src', 'models', 'anomaly_model.pkl'))
    print(">>> Model başarıyla yeniden eğitildi ve kaydedildi!")





def train_model():
    # 1. Veri yolunu belirle
    data_path = 'data/raw_sensor_data.csv'
    
    # Veri seti yoksa hata ver
    if not os.path.exists(data_path):
        print("Hata: 'data/raw_sensor_data.csv' bulunamadı! Lütfen önce collector.py çalıştırın.")
        return

    # 2. Veriyi yükle ve özellik seçimi yap
    df = pd.read_csv(data_path)
    X = df[['temperature', 'pressure']]

    # 3. Isolation Forest modelini tanımla
    model = IsolationForest(contamination=0.05, random_state=42)
    model.fit(X)

    # 4. Modeli kaydet
    model_dir = 'src/models'
    if not os.path.exists(model_dir):
        os.makedirs(model_dir)
        
    joblib.dump(model, os.path.join(model_dir, 'anomaly_model.pkl'))
    print(">>> anomaly_model.pkl başarıyla oluşturuldu ve kaydedildi!")

if __name__ == "__main__":
    train_model()