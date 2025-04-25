import csv
import os
import time
from collections import defaultdict
from functools import reduce

base_dir = os.path.dirname(__file__)  # folder, w którym jest ten skrypt
csv_path = os.path.join(base_dir, "data", "netflix_titles.csv")
output_path = os.path.join(base_dir, "data", "python_output.csv")  # plik wyjściowy

# Start pomiaru czasu
start_time = time.time()

# Wczytaj dane i wykonaj map + reduce
with open(csv_path, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    movies = filter(lambda row: row["type"] == "Movie", reader)

    # map: wydobywamy tylko rok
    movie_years = map(lambda row: row["release_year"], movies)

    # reduce: zliczamy wystąpienia lat
    def count_years(acc, year):
        acc[year] += 1
        return acc

    year_count = reduce(count_years, movie_years, defaultdict(int))


# Posortowane wyniki
sorted_results = sorted(year_count.items(), key=lambda x: x[0])

# Wyświetlenie wyników
print("Liczba filmów na rok (Python):")
for year, count in sorted_results:
    print(f"{year}: {count}")

end_time = time.time()
execution_time = end_time - start_time

# Zapisujemy czas wykonania
with open("data/times.csv", "a") as f:
    f.write(f"MapReduce,{execution_time:.4f}\n")

print(f"Czas wykonania (MapReduce): {execution_time:.4f} sekundy")

# Zapisanie wyników do CSV
with open(output_path, mode="w", newline='', encoding="utf-8") as f_out:
    writer = csv.writer(f_out)
    writer.writerow(["release_year", "count"])  # nagłówki
    writer.writerows(sorted_results)

print(f"\nWyniki zapisane do: {output_path}")
