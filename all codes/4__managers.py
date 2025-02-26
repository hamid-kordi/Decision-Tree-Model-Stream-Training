import psycopg2
import json
from kafka import KafkaProducer
import time
from prometheus_client import start_http_server, Counter, Histogram

# Database connection
conn = psycopg2.connect(
    host="localhost",
    database="mydb",
    user="hamid",
    password="1234",
    port=5432
)
cursor = conn.cursor()

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Prometheus metrics
MESSAGE_SENT_COUNT = Counter('kafka_message_sent_count_manager', 'Number of messages sent to Kafka manager')
RECORDS_FETCHED_COUNT = Counter('db_records_fetched_count_manager', 'Number of records fetched from database manager')
BATCH_PROCESSING_LATENCY = Histogram('db_batch_processing_latency_seconds_manager', 'Time taken to process a batch of records manager')

offset = 0
batch_size = 1000

# Start Prometheus HTTP server to expose metrics
start_http_server(8004)  # Exposes metrics at http://localhost:8000/metrics

def fetch_and_send_data():
    global offset
    while True:
        select_query = f"SELECT * FROM FlightDetails OFFSET {offset} LIMIT {batch_size}"
        cursor.execute(select_query)
        rows = cursor.fetchall()

        if not rows:
            break
        
        # Record number of records fetched
        RECORDS_FETCHED_COUNT.inc(len(rows))

        # Start measuring batch processing time
        with BATCH_PROCESSING_LATENCY.time():
            for row in rows:
                csv_data = {
                    "Index": row[0],
                    "Quarter": row[1],
                    "Month": row[2],
                    "DayofMonth": row[3],
                    "DayOfWeek": row[4],
                    "FlightDate": str(row[5]),
                    "Airlines": row[6],
                    "OriginCityName": row[7],
                    "DestCityName": row[8],
                    "DepDelay": row[9],
                    "ArrDelay": row[10],
                    "AirTime": row[11],
                    "Distance": row[12],
                    "Month_Str": row[13],
                    "DayOfWeek_Str": row[14],
                    "Flight_Status": row[15]
                }
                producer.send('topic_M', value=csv_data)
                print(f"Sent data: {csv_data}")
                
                # Record number of messages sent to Kafka
                MESSAGE_SENT_COUNT.inc()

        offset += batch_size
        time.sleep(1)

if __name__ == "__main__":
    fetch_and_send_data()
    cursor.close()
    conn.close()
    producer.close()
