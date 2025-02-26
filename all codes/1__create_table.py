import psycopg2

# Database configuration
db_config = {
    "host": "localhost",
    "database": "mydb",
    "user": "hamid",
    "password": "1234",
    "port": 5432
}

try:
    # Establish connection
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Create FlightDetails table with appropriate data types
    create_table_query = """
    CREATE TABLE FlightDetails (
        Index VARCHAR(256),
        Quarter VARCHAR(256),
        Month VARCHAR(256),
        DayofMonth VARCHAR(256),
        DayOfWeek VARCHAR(256),
        FlightDate DATE,                        
        Airlines VARCHAR(256),
        OriginCityName VARCHAR(256),
        DestCityName VARCHAR(256),
        DepDelay DOUBLE PRECISION,                           
        ArrDelay DOUBLE PRECISION,                           
        AirTime DOUBLE PRECISION,                            
        Distance DOUBLE PRECISION,                           
        Month_Str VARCHAR(256),                   
        DayOfWeek_Str VARCHAR(256),               
        Flight_Status VARCHAR(256)
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Create accuracy_data table template
    create_result_table_query = """
    CREATE TABLE accuracy_data_model (
        id SERIAL PRIMARY KEY,
        rmse DOUBLE PRECISION NOT NULL,
        timestamp BIGINT NOT NULL,
        time TIMESTAMP NOT NULL
    );
    """
    
    for i in range(1, 5):
        table_name = f"accuracy_data_model{i}"
        cursor.execute(create_result_table_query.replace("accuracy_data_model", table_name))
        conn.commit()

    print("Tables created successfully.")

except Exception as e:
    print(f"Error: {e}")
finally:
    if conn:
        cursor.close()
        conn.close()
