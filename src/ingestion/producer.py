import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    api_version=(0, 10, 1),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_sensor_data():
    sensor_id = random.choice(['SENS-01', 'SENS-02', 'SENS-03'])
    is_anomaly = random.random() > 0.95
    temperature = random.uniform(80,10) if is_anomaly else random.uniform(20, 40)

    return {
        'sensor_id': sensor_id,
        'timestamp': int(time.time()),
        'temperature': round(temperature, 2),
        'pressure': round(random.uniform(1, 5), 2),
        'is_anomaly' : is_anomaly
    }

print("Starting sensor data production...")

try:
    while True:
        data = generate_sensor_data()
        producer.send('sensor-data', value=data)
        print(f"Sent data: {data}")
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopping sensor data production...")
