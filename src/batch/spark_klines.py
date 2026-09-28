import glob
import os
import zipfile
from pyspark.sql import SparkSession, functions as F, types

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")

raw_dir = "data/raw"
for zip_file in glob.glob(f"{raw_dir}/*.zip"):
    with zipfile.ZipFile(zip_file, "r") as z:
        z.extractall(raw_dir)

csv_files = [f.replace("\\", "/") for f in glob.glob(f"{raw_dir}/*.csv")]

spark = (
    SparkSession.builder.master("local[*]")
    .appName("binance-spark")
    .getOrCreate()
)

schema = types.StructType(
    [
        types.StructField("open_time", types.LongType(), True),
        types.StructField("open", types.DoubleType(), True),
        types.StructField("high", types.DoubleType(), True),
        types.StructField("low", types.DoubleType(), True),
        types.StructField("close", types.DoubleType(), True),
        types.StructField("volume", types.DoubleType(), True),
        types.StructField("close_time", types.LongType(), True),
        types.StructField("quote_asset_volume", types.DoubleType(), True),
        types.StructField("number_of_trades", types.LongType(), True),
        types.StructField("taker_buy_base_volume", types.DoubleType(), True),
        types.StructField("taker_buy_quote_volume", types.DoubleType(), True),
        types.StructField("ignore", types.StringType(), True),
    ]
)

df = spark.read.option("header", "false").schema(schema).csv(csv_files)

df_clean = (
    df.withColumn(
        "symbol", F.regexp_extract(F.input_file_name(), r"([A-Z0-9]+)-1h-", 1)
    )
    .withColumn("timestamp", F.to_timestamp(F.col("open_time") / 1000000))
    .withColumn(
        "price_change_pct",
        F.round(((F.col("close") - F.col("open")) / F.col("open")) * 100, 2),
    )
    .filter((F.col("close") > 0) & (F.col("volume") > 0))
)

lake_path = "data/lake/klines"
(
    df_clean.repartition("symbol")
    .write.mode("overwrite")
    .partitionBy("symbol")
    .parquet(lake_path)
)

spark.stop()
print("Spark: батч-обработка завершена")