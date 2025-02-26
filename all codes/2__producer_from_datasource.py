from kafka import KafkaProducer
import json
import csv
from prometheus_client import start_http_server, Counter, Histogram

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'  
KAFKA_TOPIC = 'topic_A' 

# Prometheus metrics
MESSAGE_COUNT = Counter('kafka_message_count_produse_A', 'Number of messages sent to Kafka')
MESSAGE_LATENCY = Histogram('kafka_message_latency_seconds_producer_A', 'Time taken to send messages to Kafka')

def select_topic(num):
    return "topic_A" 

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Start Prometheus HTTP server to expose metrics
start_http_server(8002)  # Exposes metrics at http://localhost:8000/metrics

csv_file_path = 'df_EDA.csv'
count = 0 

with open(csv_file_path, 'r') as file:
    csv_reader = csv.DictReader(file)  
    for row in csv_reader:
        message = {key: row[key] for key in row}
        print(message)

        # Record the start time of sending the message
        with MESSAGE_LATENCY.time():
            producer.send(topic="topic_A", value=message)
        
        MESSAGE_COUNT.inc()  # Increment message count for the specific topic
        count += 1

# Close the Kafka producer when done
producer.close()
