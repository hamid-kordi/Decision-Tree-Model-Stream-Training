import os
import shutil
import psycopg2
import time
import datetime
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql import DataFrame
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from prometheus_client import start_http_server, Counter, Histogram

os.environ['SPARK_DRIVER_EXTRA_JAVA_OPTIONS'] = '--illegal-access=permit'
os.environ['SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS'] = '--illegal-access=permit'

# Create SparkSession
spark = SparkSession.builder \
    .appName("RandomCSVReadAndModelTraining") \
    .getOrCreate()

# Prometheus metrics
MODEL_TRAINED_COUNT = Counter('model_trained_count_line_4', 'Number of models trained line 4')
MODEL_TESTED_COUNT = Counter('model_tested_count_line_4', 'Number of models tested line 4')
MODEL_ACCURACY = Histogram('model_accuracy_line_4', 'Model accuracy over time line 4')
MODEL_TRAINING_TIME = Histogram('model_training_time_seconds_line_4', 'Model training time in seconds line 4')

# CSV file paths
csv_file_path = "/home/hamidreza/Documents/bigdata/data/csv_file4.csv"
model_output_path = "/home/hamidreza/Documents/bigdata/model/model_s4"
csv_file_path_test = "/home/hamidreza/Documents/bigdata/data/csv_file1.csv"
model_output_path_test = "/home/hamidreza/Documents/bigdata/model/model_s4/final_model"

# Define schema
schema = StructType([
    StructField("Index", StringType(), True),
    StructField("Quarter", StringType(), True),
    StructField("Month", StringType(), True),
    StructField("DayofMonth", StringType(), True),
    StructField("DayOfWeek", StringType(), True),
    StructField("FlightDate", StringType(), True),
    StructField("Airlines", StringType(), True),
    StructField("OriginCityName", StringType(), True),
    StructField("DestCityName", StringType(), True),
    StructField("DepDelay", DoubleType(), True),
    StructField("ArrDelay", DoubleType(), True),
    StructField("AirTime", DoubleType(), True),
    StructField("Distance", DoubleType(), True),
    StructField("Month_Str", StringType(), True),
    StructField("DayOfWeek_Str", StringType(), True),
    StructField("Flight_Status", StringType(), True)
])

db_config = {
    "host": "localhost",
    "database": "mydb",
    "user": "hamid",
    "password": "1234",
    "port": 5432
}
start_http_server(8064)



def insert_data(cursor, data):
    insert_query = "INSERT INTO accuracy_data_model4 (rmse, timestamp, time) VALUES (%s, %s, %s)"
    cursor.execute(insert_query, data)

def connect_to_db():
    return psycopg2.connect(**db_config)

# Function to read 100 random lines from CSV
def read_random_lines(csv_file_path: str, num_samples: int = 100) -> DataFrame:
    df = spark.read.csv(csv_file_path, header=True, schema=schema)
    fraction = 300/df.count()
    sampled_df = df.sample(withReplacement=False, fraction=fraction, seed=42)
    return sampled_df

def test_model():
    conn = connect_to_db()
    cursor = conn.cursor()
    model = PipelineModel.load(model_output_path_test)

    df = spark.read.csv(csv_file_path, header=True, schema=schema)
    df.count()

    numeric_cols = ["DepDelay", "AirTime", "Distance"]
 
    df = df.select(
        *[col(c).cast(DoubleType()).alias(c) if c in numeric_cols + ["ArrDelay"] else col(c) for c in df.columns]
    )

    categorical_cols = ["Airlines", "OriginCityName", "DestCityName", "Month_Str", "DayOfWeek_Str"]
    indexers = []
    encoders = []

    for c in categorical_cols:
        distinct_values = df.select(c).distinct().count()  # Count distinct values in the column
        if distinct_values > 1:
            indexers.append(StringIndexer(inputCol=c, outputCol=f"{c}_index",handleInvalid="skip"))
            encoders.append(OneHotEncoder(inputCol=f"{c}_index", outputCol=f"{c}_encoded"))


    test_df = model.transform(df)

 
    evaluator = RegressionEvaluator(labelCol="ArrDelay", predictionCol="prediction", metricName="rmse")

    # Calculate accuracy
    #accuracy = evaluator.evaluate(test_df)
    rmse = evaluator.evaluate(test_df)
    print(f"✅ rmse: {rmse:.2f}%")
    
    # Record the accuracy metric
    MODEL_ACCURACY.observe(rmse)

    test_df.select("Index", "ArrDelay", "prediction").show()

    current_datetime = datetime.datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    accuracy_data = ()
    accuracy_data = (1-rmse , time.time(),formatted_datetime)
    insert_data(cursor, accuracy_data)
    conn.commit()

    # Increment model tested count metric
    MODEL_TESTED_COUNT.inc()

def train_model(df: DataFrame):
    # Convert numeric columns to DoubleType
    numeric_cols = ["DepDelay",  "AirTime", "Distance"]

    df = df.select(
        *[col(c).cast(DoubleType()).alias(c) if c in numeric_cols + ["ArrDelay"] else col(c) for c in df.columns]
    )

    # Convert categorical features with StringIndexer and OneHotEncoder

    categorical_cols = ["Airlines", "OriginCityName", "DestCityName", "Month_Str", "DayOfWeek_Str"]
    indexers = []
    encoders = []
    encoded_cols = []

    for c in categorical_cols:
        distinct_values = df.select(c).distinct().count()  
        if distinct_values > 1:
            indexer = StringIndexer(inputCol=c, outputCol=f"{c}_index",handleInvalid='skip')
            encoder = OneHotEncoder(inputCol=f"{c}_index", outputCol=f"{c}_encoded")

            indexers.append(indexer)
            encoders.append(encoder)
            encoded_cols.append(f"{c}_encoded")
    # Combine features

    df = df.na.fill({"Airlines": "Unknown", "OriginCityName": "Unknown", "DestCityName": "Unknown"})

    assembler = VectorAssembler(
        inputCols=encoded_cols + numeric_cols,
        outputCol="features"
    )

    label_indexer = StringIndexer(inputCol="Flight_Status", outputCol="label")

    # dt = DecisionTreeClassifier(labelCol="label", featuresCol="features", maxDepth=3, maxBins=32, minInstancesPerNode=20)
    rf = RandomForestRegressor(featuresCol="features", labelCol="ArrDelay", numTrees=50, maxDepth=5)
    pipeline = Pipeline(stages=indexers + encoders + [assembler, rf])

    # Start timing the training process
    start_time = time.time()

    # Train model
    model = pipeline.fit(df)

    # End timing the training process
    end_time = time.time()

    # Record training time metric
    MODEL_TRAINING_TIME.observe(end_time - start_time)

    # Save model with unique version (using timestamp)
    model_path = os.path.join(model_output_path, "final_model")
    if os.path.exists(model_path):
        print("Delete previous model")
        shutil.rmtree(model_path)
    model.write().overwrite().save(model_path)

    print(f"✅ New model trained and saved at {model_path}")

    # Increment model trained count metric
    MODEL_TRAINED_COUNT.inc()

while True:
    df_sample = read_random_lines(csv_file_path) 
    train_model(df_sample)
    #  
    test_model()
