from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import time
import os

# Inicjalizacja sesji Spark
spark = SparkSession.builder \
    .appName("FlightDataPySpark") \
    .getOrCreate()

# Start pomiaru czasu
start_time = time.time()

# Ścieżka do pliku CSV
file_path = "data/flight_data.csv"  # <- Upewnij się, że plik jest w tej lokalizacji

# Wczytanie danych
df = spark.read.option("header", True).csv(file_path)

# Oczyszczenie danych: usunięcie pustych wartości w dest_country
df_clean = df.filter(col("dest_country").isNotNull() & (col("dest_country") != ""))

# Agregacja: liczba lotów do każdego kraju docelowego
df_result = df_clean.groupBy("dest_country").agg(count("*").alias("count")).orderBy("dest_country")

# Wyświetlenie części wyniku
print("\nLiczba lotów do krajów docelowych (PySpark):")
df_result.show(10)

# Pomiar czasu wykonania
execution_time = time.time() - start_time

# Zapis czasu do pliku
with open("data/times.csv", "a") as f:
    f.write(f"PySpark_DestCountry,{execution_time:.4f}\n")

print(f"Czas wykonania (PySpark): {execution_time:.4f} sekundy")

# Zapisanie wyników do CSV
output_path = "data/pyspark_dest_country_output.csv"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

df_result.toPandas().to_csv(output_path, index=False)
print(f"\nWyniki zapisane do: {output_path}")