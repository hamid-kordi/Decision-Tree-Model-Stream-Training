
# Data Stream Pipeline for Decision Tree Training

This project builds a Big Data pipeline to train ML models using Kafka, Spark, PostgreSQL, Docker, Prometheus, and Grafana.

## 🔧 Stack
- Kafka: Real-time data streaming
- Spark (PySpark): ML model training
- PostgreSQL (Dockerized): Data storage
- Prometheus + Grafana: Monitoring
- Python: Control scripts

## 🎯 Goal
Predict flight delays using a Decision Tree Classifier on 2022 US domestic flight data.

## 🔄 Pipeline Steps
1. **Kafka Producer**: Reads CSV → sends to `topic_A`
2. **Kafka Consumer**: Reads from `topic_A` → writes to PostgreSQL
3. **Data Exporter**: Reads from DB → sends to `topic_M`
4. **CSV Splitter**: Writes messages to 4 CSV files
5. **Model Training**: Trains 4 Spark models
6. **Model Evaluation**: Logs RMSE to PostgreSQL
7. **Monitoring**: Prometheus metrics + Grafana dashboards

## 📁 Structure
```
├── producer/              # kafka_producer.py
├── consumer/              # kafka_consumer.py
├── database/              # save_to_db.py
├── pipeline/              # model_train.py, model_test.py, csv_splitter.py
├── monitoring/            # Prometheus & Grafana configs
└── docker/                # docker-compose.yml
```

## ▶️ Run
```bash
docker-compose up -d
python producer/kafka_producer.py
python consumer/kafka_consumer.py
python pipeline/csv_splitter.py
python pipeline/model_train.py
python pipeline/model_test.py
```

Access Grafana at: http://localhost:3000 (admin/admin)

## 👤 Author
**Hamidreza Kordi** – Iran University of Science and Technology (IUST)  
Fall 1403 | Supervisor: Dr. Naderi 
