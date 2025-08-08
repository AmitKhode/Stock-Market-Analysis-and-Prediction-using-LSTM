from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col
import pandas as pd
import os
import subprocess

# CONFIG
HDFS_BASE_PATH = "hdfs://localhost:9000/user/amitk/stock_data_cleaned"
LOCAL_OUTPUT_DIR = "/home/amitk/Desktop/stock-market-etl/spark_output"
SYMBOLS = ["AAPL", "GOOG", "MSFT", "AMZN", "TSLA"]

os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)

# Initialize Spark
spark = SparkSession.builder \
    .appName("SafeExportCleanedDataFromHDFS") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

for symbol in SYMBOLS:
    print(f"📥 Exporting {symbol} from HDFS...")
    hdfs_symbol_path = f"{HDFS_BASE_PATH}/symbol={symbol}"
    local_path = os.path.join(LOCAL_OUTPUT_DIR, f"{symbol}_cleaned.csv")

    try:
        # ✅ List all files under symbol's partition folder
        result = subprocess.run(
            ["hdfs", "dfs", "-ls", hdfs_symbol_path],
            capture_output=True, text=True
        )
        lines = result.stdout.strip().split("\n")[1:]  # skip header
        parquet_files = [line.split()[-1] for line in lines if line.endswith(".parquet") or ".snappy.parquet" in line]

        if not parquet_files:
            print(f"⚠️ No Parquet files found for {symbol}")
            continue

        # ✅ Read and cast each file individually, then union
        df_total = None
        for path in parquet_files:
            df_part = spark.read.parquet(path)
            df_part = df_part.withColumn("Volume", col("Volume").cast(DoubleType()))
            df_total = df_part if df_total is None else df_total.unionByName(df_part)

        if df_total is None:
            print(f"⚠️ No data loaded for {symbol}")
            continue

        pdf = df_total.toPandas()
        pdf["timestamp"] = pd.to_datetime(pdf["timestamp"])
        pdf.sort_values("timestamp", inplace=True)
        pdf.to_csv(local_path, index=False)
        print(f"✅ Exported {len(pdf)} rows for {symbol} to CSV")

    except Exception as e:
        print(f"[ERROR] Could not export {symbol}: {e}")

spark.stop()
print("🏁 Export complete.")
