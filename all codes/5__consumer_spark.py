import csv
from kafka import KafkaConsumer
import json
from prometheus_client import start_http_server, Counter

# Kafka Consumer Setup
consumer = KafkaConsumer(
    'topic_M',
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
)

# Prometheus metrics
MESSAGE_CONSUMED_COUNT = Counter('kafka_message_consumed_count', 'Number of messages consumed from Kafka', ['topic'])
ROW_WRITTEN_COUNT = Counter('csv_row_written_count', 'Number of rows written to CSV files', ['file'])

# File paths
csv_files = [
    "/home/hamidreza/Documents/bigdata/data/csv_file1.csv",
    "/home/hamidreza/Documents/bigdata/data/csv_file2.csv",
    "/home/hamidreza/Documents/bigdata/data/csv_file3.csv",
    "/home/hamidreza/Documents/bigdata/data/csv_file4.csv"
]

headers = [
    "Index", "Quarter", "Month", "DayofMonth", "DayOfWeek", "FlightDate", "Airlines",
    "OriginCityName", "DestCityName", "DepDelay", "ArrDelay", "AirTime", "Distance",
    "Month_Str", "DayOfWeek_Str", "Flight_Status"
]

# Ensure CSV headers exist
for path in csv_files:
    try:
        with open(path, 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            if file.tell() == 0:
                writer.writerow(headers)
    except Exception as e:
        print(f"Error initializing file {path}: {e}")

# Start Prometheus HTTP server to expose metrics
start_http_server(8000)  # Exposes metrics at http://localhost:8000/metrics

# Process Kafka messages
count = 0
try:
    for message in consumer:
        data = message.value
        count += 1
        row = [data.get(header) for header in headers]
        
        # Record the message consumption metric
        MESSAGE_CONSUMED_COUNT.labels(topic='topic_M').inc()
        
        file_index = count % len(csv_files)
        with open(csv_files[file_index], 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(row)
            print(f"Added to {csv_files[file_index]}")
        
            # Record the row written to the respective CSV file
            ROW_WRITTEN_COUNT.labels(file=csv_files[file_index]).inc()

except KeyboardInterrupt:
    print("Consumer stopped manually")
except Exception as e:
    print(f"Error while consuming Kafka messages: {e}")
finally:
    consumer.close()
