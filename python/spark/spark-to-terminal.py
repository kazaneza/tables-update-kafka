from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("PySparkTest").getOrCreate()

# Simple data for testing
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

# Create DataFrame
columns = ["Language", "Users"]
df = spark.createDataFrame(data, columns)

# Show DataFrame
df.show()

# Stop the Spark session
spark.stop()
