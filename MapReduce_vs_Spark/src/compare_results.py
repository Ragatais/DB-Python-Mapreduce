import pandas as pd
import matplotlib.pyplot as plt

# Ścieżki do plików
pyspark_file = "data/pyspark_output.csv"
python_file = "data/python_output.csv"
comparison_file = "data/comparison_results.csv"

# Wczytanie danych
pyspark_df = pd.read_csv(pyspark_file)
python_df = pd.read_csv(python_file)

# Wczytanie pliku z czasami
times_df = pd.read_csv("data/times.csv", header=None, names=["Algorytm", "Czas"])

# Merging danych na podstawie release_year
comparison_df = pyspark_df.merge(
    python_df, on="release_year", how="outer", suffixes=("_pyspark", "_python")
)

# Wypełnienie brakujących wartości zerami
comparison_df = comparison_df.fillna(0)

# Dodanie kolumny z różnicą
comparison_df["difference"] = (
    comparison_df["count_pyspark"] - comparison_df["count_python"]
)

# Zapisanie do pliku CSV
comparison_df.to_csv(comparison_file, index=False)

# Wykres porównawczy
plt.figure(figsize=(10, 5))
plt.plot(comparison_df["release_year"], comparison_df["count_pyspark"], label="PySpark", marker="o")
plt.plot(comparison_df["release_year"], comparison_df["count_python"], label="MapReduce", marker="s")
plt.xlabel("Rok wydania")
plt.ylabel("Liczba filmów")
plt.title("Porównanie liczby filmów na rok (PySpark vs MapReduce)")
plt.legend()
plt.grid()
plt.xticks(rotation=45)
plt.savefig("data/comparison_plot.png")

# Wykres czasów wykonania
plt.figure(figsize=(6, 4))
plt.bar(times_df["Algorytm"], times_df["Czas"], color=["blue", "orange"])
plt.ylabel("Czas (sekundy)")
plt.title("Porównanie czasu wykonania PySpark vs MapReduce")

# Zapisanie wykresu
plt.savefig("data/time_comparison.png")
