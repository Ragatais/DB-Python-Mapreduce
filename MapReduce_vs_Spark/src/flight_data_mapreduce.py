import csv
import os
import time
from collections import defaultdict
from functools import reduce

# Ścieżki plików
base_dir = os.path.dirname(__file__)
csv_path = os.path.join(base_dir, "data", "flight_data.csv")  # <-- nazwij odpowiednio plik
output_path = os.path.join(base_dir, "data", "dest_country_output.csv")

# Start pomiaru czasu
start_time = time.time()

with open(csv_path, encoding="utf-8") as f:
    reader = csv.DictReader(f)

    # filtr: pomijamy puste kraje docelowe (jeśli występują)
    valid_rows = filter(lambda row: row["dest_country"].strip() != "", reader)

    # map: tylko kolumna 'dest_country'
    dest_countries = map(lambda row: row["dest_country"], valid_rows)

    # reduce: liczenie wystąpień krajów docelowych
    def count_countries(acc, country):
        acc[country] += 1
        return acc

    country_count = reduce(count_countries, dest_countries, defaultdict(int))
# Posortowane wyniki alfabetycznie po kraju
sorted_results = sorted(country_count.items(), key=lambda x: x[0])

# Wyświetlenie wyników
print("Liczba lotów do krajów docelowych:")
for country, count in sorted_results:
    print(f"{country}: {count}")

# Pomiar czasu wykonania
execution_time = time.time() - start_time

# Zapis czasu
with open(os.path.join(base_dir, "data", "times.csv"), "a") as f:
    f.write(f"DestCountryMapReduce,{execution_time:.4f}\n")

# Zapis wyników do CSV
with open(output_path, mode="w", newline='', encoding="utf-8") as f_out:
    writer = csv.writer(f_out)
    writer.writerow(["dest_country", "count"])
    writer.writerows(sorted_results)

print(f"\nWyniki zapisane do: {output_path}")
print(f"Czas wykonania: {execution_time:.4f} sekundy")