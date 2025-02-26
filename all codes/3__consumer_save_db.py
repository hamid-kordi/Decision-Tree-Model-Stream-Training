from kafka import KafkaConsumer
import json
import logging
import psycopg2
from prometheus_client import start_http_server, Counter, Histogram

# Database configuration
db_config = {
    "host": "localhost",
    "database": "mydb",
    "user": "hamid",
    "password": "1234",
    "port": 5432
}

# Prometheus metrics
MESSAGE_CONSUMED_COUNT = Counter('kafka_message_consumed_count_save_db', 'Number of messages consumed from Kafka')
INSERT_BATCH_COUNT = Counter('kafka_insert_batch_count_save_db', 'Number of data batches inserted into the database')
ERROR_COUNT = Counter('kafka_consumer_error_count_save_db', 'Number of errors encountered by the Kafka consumer')
MESSAGE_PROCESSING_LATENCY = Histogram('kafka_message_processing_latency_seconds_save_db', 'Time taken to process a message')

def insert_data_batch(cursor, data_batch):
    insert_query = """
    INSERT INTO FlightDetails(Index, Quarter, Month, DayofMonth, DayOfWeek, FlightDate, Airlines, OriginCityName, DestCityName, DepDelay, ArrDelay, AirTime, Distance, Month_Str, DayOfWeek_Str, Flight_Status)    
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    for data in data_batch:
        cursor.execute(insert_query, data)

def connect_to_db():
    return psycopg2.connect(**db_config)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

consumer = KafkaConsumer(
    'topic_A',  
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'  
)

# Start Prometheus HTTP server to expose metrics
start_http_server(8003)  # Exposes metrics at http://localhost:8000/metrics

def main():
    conn = connect_to_db()
    cursor = conn.cursor()
    data_batch = []
    
    try:
        for message in consumer:
            # Start measuring message processing time
            with MESSAGE_PROCESSING_LATENCY.time():
                data = message.value 
                data_batch.append((data["Index"], data["Quarter"],
                                   data["Month"], data["DayofMonth"], data["DayOfWeek"],
                                   data["FlightDate"], data["Airlines"], data["OriginCityName"],
                                   data["DestCityName"], data["DepDelay"], data["ArrDelay"], data["AirTime"],
                                   data["Distance"], data["Month_Str"], data["DayOfWeek_Str"], data["Flight_Status"]))
            
            # Record message consumption
            MESSAGE_CONSUMED_COUNT.inc()
            
            if len(data_batch) >= 5:
                insert_data_batch(cursor, data_batch)
                conn.commit()
                data_batch.clear()
                INSERT_BATCH_COUNT.inc()  # Increment the batch insert counter
            
            print(message)
    
    except Exception as e:
        logging.error(f"Error in Kafka consumer: {e}")
        ERROR_COUNT.inc()  # Increment error counter
    finally:
        logging.info("Shutting down Kafka consumer.")
        consumer.close()
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
