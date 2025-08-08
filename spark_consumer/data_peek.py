from pyspark.sql import SparkSession

# Create SparkSession in local mode
spark = SparkSession.builder \
    .appName("PeekStockData") \
    .master("local[*]") \
    .getOrCreate()

# Set log level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Read the Parquet data from HDFS
df = spark.read.parquet("hdfs://localhost:9000/user/amitk/stock_data_cleaned/")

# Show the top 20 rows sorted by timestamp (most recent first)
df.select("symbol", "timestamp", "Open", "High", "Low", "Close", "Volume") \
  .orderBy("timestamp", ascending=False) \
  .show(20, truncate=False)

# df.filter(df.symbol == "TSLA").show()

# Print the schema of the DataFrame
df.printSchema()
