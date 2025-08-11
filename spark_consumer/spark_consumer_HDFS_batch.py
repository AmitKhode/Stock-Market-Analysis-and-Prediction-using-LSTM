from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os
import pandas as pd

# --- CONFIG ---
KAFKA_TOPIC = "stock_topic"
KAFKA_SERVERS = "localhost:9092"
HDFS_OUTPUT_PATH = "hdfs://localhost:9000/user/amitk/stock_data_cleaned"
LOCAL_OUTPUT_DIR = "/home/amitk/Desktop/stock-market-etl/spark_output"

os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)

# --- Initialize Spark ---
spark = SparkSession.builder \
    .appName("KafkaSparkStockETL_Batch") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- Define Schema ---
schema = StructType([
    StructField("symbol", StringType()),
    StructField("timestamp", StringType()),
    StructField("Open", DoubleType()),
    StructField("High", DoubleType()),
    StructField("Low", DoubleType()),
    StructField("Close", DoubleType()),
    StructField("Volume", DoubleType())  # Force double to avoid INT merge issues
])

# --- Read from Kafka (BATCH MODE) ---
df_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# --- Parse JSON ---
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# --- Clean and filter ---
df_cleaned = df_parsed \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("Volume", col("Volume").cast(DoubleType())) \
    .dropna(subset=["timestamp", "symbol", "Close"]) \
    .filter((col("Close") > 0) & (col("Volume") > 0))

total_rows = df_cleaned.count()
if total_rows == 0:
    print("⚠️ No data to process. Exiting.")
    spark.stop()
    exit(0)

print(f"✅ Processing {total_rows} rows...")

# --- Write to HDFS ---
df_cleaned.write \
    .mode("append") \
    .partitionBy("symbol") \
    .parquet(HDFS_OUTPUT_PATH)
print(f"📦 Data written to HDFS: {HDFS_OUTPUT_PATH}")

# --- Write to local CSVs ---
symbols = [row.symbol for row in df_cleaned.select("symbol").distinct().collect()]
for symbol in symbols:
    try:
        df_filtered = df_cleaned.filter(col("symbol") == symbol).orderBy("timestamp")
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

        print(f"[{symbol}] ➕ Appended {len(new)} rows to {csv_path}")

    except Exception as e:
        print(f"[ERROR] Failed to write CSV for {symbol}: {e}")

print("🏁 Batch consumer finished.")
spark.stop()
