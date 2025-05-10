from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import time
import os

spark = SparkSession.builder \
    .appName("NetflixPySpark") \
    .getOrCreate()

start_time = time.time()

file_path = "data/netflix_titles.csv"
df = spark.read.option("header", True).csv(file_path)

df_clean = df.withColumn("release_year", col("release_year").cast("int")) \
             .filter((col("release_year").isNotNull()) & (col("type") == "Movie"))

df_result = df_clean.groupBy("release_year").agg(count("*").alias("count")).orderBy("release_year")

print("\nLiczba filmów na rok (PySpark):")
df_result.show(10)

end_time = time.time()
execution_time = end_time - start_time

with open("data/times.csv", "a") as f:
    f.write(f"PySpark,{execution_time:.4f}\n")

print(f"Czas wykonania (PySpark): {execution_time:.4f} sekundy")

output_path = "data/pyspark_output.csv"
output_dir = os.path.dirname(output_path)
os.makedirs(output_dir, exist_ok=True)

df_result.toPandas().to_csv(output_path, index=False)
print(f"\nWyniki zapisane do: {output_path}")
