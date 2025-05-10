from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import time
import os

spark = SparkSession.builder \
    .appName("FlightDataPySpark") \
    .getOrCreate()

start_time = time.time()

file_path = "data/flight_data.csv"  # <- Upewnij się, że plik jest w tej lokalizacji

df = spark.read.option("header", True).csv(file_path)

df_clean = df.filter(col("dest_country").isNotNull() & (col("dest_country") != ""))

df_result = df_clean.groupBy("dest_country").agg(count("*").alias("count")).orderBy("dest_country")

print("\nLiczba lotów do krajów docelowych (PySpark):")
df_result.show(10)

execution_time = time.time() - start_time

with open("data/times.csv", "a") as f:
    f.write(f"PySpark_DestCountry,{execution_time:.4f}\n")

print(f"Czas wykonania (PySpark): {execution_time:.4f} sekundy")

output_path = "data/pyspark_dest_country_output.csv"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

df_result.toPandas().to_csv(output_path, index=False)
print(f"\nWyniki zapisane do: {output_path}")