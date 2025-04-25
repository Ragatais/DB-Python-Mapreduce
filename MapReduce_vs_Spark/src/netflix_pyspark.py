from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import time
import os

# Inicjalizacja sesji Spark
spark = SparkSession.builder \
    .appName("NetflixPySpark") \
    .getOrCreate()

# Start pomiaru czasu
start_time = time.time()

# Wczytanie danych z CSV
file_path = "data/netflix_titles.csv"
df = spark.read.option("header", True).csv(file_path)


# Konwersja kolumny release_year na integer i filtrowanie nieprawidłowych wartości
df_clean = df.withColumn("release_year", col("release_year").cast("int")) \
             .filter((col("release_year").isNotNull()) & (col("type") == "Movie"))

# Agregacja: liczba filmów na każdy rok
df_result = df_clean.groupBy("release_year").agg(count("*").alias("count")).orderBy("release_year")

# Wyświetlenie wyniku
print("\nLiczba filmów na rok (PySpark):")
df_result.show(10)

# Koniec pomiaru czasu
end_time = time.time()
execution_time = end_time - start_time

# Zapisujemy czas wykonania
with open("data/times.csv", "a") as f:
    f.write(f"PySpark,{execution_time:.4f}\n")

print(f"Czas wykonania (PySpark): {execution_time:.4f} sekundy")

# Zapisanie wyników do CSV
output_path = "data/pyspark_output.csv"
output_dir = os.path.dirname(output_path)
os.makedirs(output_dir, exist_ok=True)

df_result.toPandas().to_csv(output_path, index=False)
print(f"\nWyniki zapisane do: {output_path}")
