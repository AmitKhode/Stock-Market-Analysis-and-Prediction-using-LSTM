from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os
import pandas as pd

# --- CONFIG ---
KAFKA_TOPIC = "stock_topic"
KAFKA_SERVERS = "localhost:9092"
HDFS_OUTPUT_PATH = "hdfs://localhost:9000/user/amitk/stock_data_cleaned"
HDFS_CHECKPOINT = "hdfs://localhost:9000/user/amitk/checkpoints/stock_data_cleaned"
LOCAL_OUTPUT_DIR = "/home/amitk/Desktop/stock-market-etl/spark_output"

os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)

# --- Initialize Spark ---
spark = SparkSession.builder \
    .appName("KafkaSparkStockETL") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- Define Schema for incoming Kafka messages ---
schema = StructType([
    StructField("symbol", StringType()),
    StructField("timestamp", StringType()),
    StructField("Open", DoubleType()),
    StructField("High", DoubleType()),
    StructField("Low", DoubleType()),
    StructField("Close", DoubleType()),
    StructField("Volume", DoubleType())
])

# --- Read from Kafka topic ---
df_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# --- Parse Kafka JSON payload ---
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# --- Clean and filter data ---
df_cleaned = df_parsed \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .dropna(subset=["timestamp", "symbol", "Close"]) \
    .filter((col("Close") > 0) & (col("Volume") > 0))

# --- Write to HDFS in Parquet format (per symbol) ---
query_hdfs = df_cleaned.writeStream \
    .format("parquet") \
    .option("path", HDFS_OUTPUT_PATH) \
    .option("checkpointLocation", HDFS_CHECKPOINT) \
    .partitionBy("symbol") \
    .outputMode("append") \
    .start()

# --- Local CSV writer function ---
def write_to_csv(batch_df, batch_id):
    total_rows = batch_df.count()
    if total_rows == 0:
        print(f"[Batch {batch_id}] No data received.")
        return

    symbols = [row.symbol for row in batch_df.select("symbol").distinct().collect()]
    print(f"[Batch {batch_id}] Writing {total_rows} rows for symbols: {symbols}")

    for symbol in symbols:
        try:
            df_filtered = batch_df.filter(col("symbol") == symbol).orderBy("timestamp")
            new = df_filtered.toPandas()
            new["timestamp"] = pd.to_datetime(new["timestamp"])

            csv_path = os.path.join(LOCAL_OUTPUT_DIR, f"{symbol}_cleaned.csv")

            if os.path.exists(csv_path):
                existing = pd.read_csv(csv_path, parse_dates=["timestamp"])
                combined = pd.concat([existing, new], ignore_index=True)
                combined.drop_duplicates(subset=["timestamp"], inplace=True)
                combined.sort_values("timestamp", inplace=True)
                combined.to_csv(csv_path, index=False)
            else:
                new.sort_values("timestamp").to_csv(csv_path, index=False)

            print(f"[{symbol}] ➕ Appended {len(new)} new rows")

        except Exception as e:
            print(f"[ERROR] Failed to write CSV for {symbol}: {e}")

# --- Write stream to local CSVs ---
query_csv = df_cleaned.writeStream \
    .foreachBatch(write_to_csv) \
    .outputMode("append") \
    .start()

# --- Await both outputs ---
query_hdfs.awaitTermination()
query_csv.awaitTermination()
