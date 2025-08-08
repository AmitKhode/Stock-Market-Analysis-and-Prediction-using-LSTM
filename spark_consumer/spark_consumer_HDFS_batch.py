from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType

# Kafka & HDFS config
KAFKA_TOPIC = "stock_topic"
KAFKA_SERVER = "localhost:9092"
HDFS_OUTPUT_PATH = "hdfs://localhost:9000/user/amitk/stock_data/"

# Define schema matching the producer JSON
schema = StructType() \
    .add("symbol", StringType()) \
    .add("timestamp", StringType()) \
    .add("Open", DoubleType()) \
    .add("High", DoubleType()) \
    .add("Low", DoubleType()) \
    .add("Close", DoubleType()) \
    .add("Volume", DoubleType())

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("StockKafkaBatchConsumerToHDFS") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka once (batch mode)
kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Parse the Kafka JSON message
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# Write to HDFS in Parquet format, partitioned by symbol
parsed_df.write \
    .mode("append") \
    .partitionBy("symbol") \
    .parquet(HDFS_OUTPUT_PATH)

print("✅ Data written to HDFS and exiting.")
spark.stop()
