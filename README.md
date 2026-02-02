# Real-Time Anomaly Detection & Autonomous MLOps Pipeline
This project implements an end-to-end, real-time anomaly detection pipeline using Kafka, Scikit-Learn, and Docker. It features a self-adaptive MLOps loop that automatically retrains the machine learning model as new data accumulates in a SQLite database.
## 🚀 Key Features
* Real-Time Data Ingestion: Utilizes Apache Kafka to stream high-frequency sensor data (Temperature & Pressure).

* Autonomous Model Retraining: Implements a "Watchdog" mechanism that triggers retrain_model() when a specific data threshold is reached in SQLite, preventing model drift.

* AI-Powered Detection: Leverages the Isolation Forest algorithm for unsupervised anomaly detection in streaming data.

* Full Containerization: The entire infrastructure (Kafka, Zookeeper, Prometheus, Grafana) is orchestrated via Docker Compose.

* Live Monitoring: A professional Grafana dashboard visualizes real-time metrics and historical anomaly trends.

## 🛠 Tech Stack
* Language: Python 3.9+

* Machine Learning: Scikit-Learn (Isolation Forest), Joblib

* Data Streaming: Apache Kafka, Docker Desktop

* Database: SQLite3 (for persistent storage and retraining)

* Visualization: Grafana, Prometheus

* DevOps: Docker, Docker Compose

## 🏗 System Architecture
* Producer: Simulates industrial sensor data and publishes to Kafka topics.

* Stream Processor: Consumes data, performs real-time inference, and stores results in SQLite.

* Self-Learning Loop: Monitors database growth and executes an in-process retraining pipeline to update the model weights.

* Monitoring: Grafana queries SQLite to display live anomaly alerts and sensor health.

## 🚦 Getting Started
**Prerequisites**
* Docker Desktop

* Python 3.9 (Virtual Environment recommended)

**Installation**

Clone the repository:
```
git clone https://github.com/yourusername/realtime-anomaly-detection.git
```

Spin up the infrastructure:

```
docker-compose up -d
```
Install dependencies:


```
pip install -r requirements.txt
```
**Execution**

Start the Data Producer:


```
python src/ingestion/producer.py
```
Start the Stream Processor (AI Engine):

```
python src/processing/stream_processor.py
```
## 📊 MLOps Insights
This project demonstrates Continuous Training (CT) patterns. By integrating the training logic directly into the production stream, the system ensures that the Isolation Forest model adapts to seasonal changes in sensor behavior without manual intervention.
