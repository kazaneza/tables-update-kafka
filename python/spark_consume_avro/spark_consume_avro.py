import requests
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col

def list_schema_registry_subjects(schema_registry_url):
    response = requests.get(f"{schema_registry_url}/subjects")
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to list subjects: {response.text}")

def fetch_schema_from_registry(schema_registry_url, subject):
    response = requests.get(f"{schema_registry_url}/subjects/{subject}/versions/latest")
    if response.status_code == 200:
        return response.json()['schema']
    else:
        raise Exception(f"Failed to get schema: {response.text}")

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("KafkaAvroRead") \
    .getOrCreate()

# Schema Registry URL
schema_registry_url = "http://10.24.36.25:35003"
kafka_topic = "table-update"

# List all subjects
subjects = list_schema_registry_subjects(schema_registry_url)

# Find the correct subject for the Kafka topic
subject_for_topic = next((sub for sub in subjects if kafka_topic in sub), None)
if not subject_for_topic:
    raise Exception(f"No subject found for topic {kafka_topic}")

# Fetch the Avro schema for the subject
avro_schema = fetch_schema_from_registry(schema_registry_url, subject_for_topic)

# Kafka Configuration
kafka_bootstrap_servers = "10.24.36.25:35002"

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Deserialize the Avro data
df_deserialized = df.select(from_avro(col("value"), avro_schema).alias("data"))

# Flatten the data and select fields
df_flattened = df_deserialized.selectExpr("data.*")

# Print the deserialized data
query = df_flattened \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
