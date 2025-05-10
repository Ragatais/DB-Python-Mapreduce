from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import time
import os
import glob

spark = SparkSession.builder \
    .appName("TxIdxCountFromChunks") \
    .getOrCreate()

start_time = time.time()

base_dir = os.path.dirname(__file__)
data_dir = os.path.join(base_dir, "data")

all_chunks = []
for i in range(1, 21):
    file_path = os.path.join(data_dir, f"chunk_{i}.csv")
    if os.path.exists(file_path):
        all_chunks.append(file_path)

print(all_chunks)

df = spark.read.option("header", True).csv(all_chunks)

df_clean = df.filter(col("tx_idx").isNotNull() & (col("tx_idx") != ""))

df_result = df_clean.groupBy("tx_idx").agg(count("*").alias("count")).orderBy("tx_idx")

print("\nLiczba wystąpień tx_idx (PySpark):")
df_result.show(10)

execution_time = time.time() - start_time

with open(os.path.join(data_dir, "times.csv"), "a") as f:
    f.write(f"PySpark_TxIdx_MultiFile,{execution_time:.4f}\n")

print(f"Czas wykonania: {execution_time:.4f} sekundy")

output_path = os.path.join(data_dir, "pyspark_tx_idx_output.csv")
os.makedirs(os.path.dirname(output_path), exist_ok=True)
df_result.toPandas().to_csv(output_path, index=False)

print(f"\nWyniki zapisane do: {output_path}")