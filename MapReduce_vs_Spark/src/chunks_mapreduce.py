import csv
import os
import time
from collections import defaultdict
from functools import reduce
from glob import glob

base_dir = os.path.dirname(__file__)
input_pattern = os.path.join(base_dir, "data", "chunk_*.csv")
output_path = os.path.join(base_dir, "data", "tx_idx_output.csv")

start_time = time.time()

file_list = glob(input_pattern)

def count_tx(acc, tx_idx):
    acc[tx_idx] += 1
    return acc

all_tx_indices = []

for csv_path in file_list:
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        valid_rows = filter(lambda row: row["tx_idx"].strip() != "", reader)
        tx_indices = map(lambda row: row["tx_idx"], valid_rows)
        all_tx_indices.extend(tx_indices)

tx_count = reduce(count_tx, all_tx_indices, defaultdict(int))

sorted_results = sorted(tx_count.items(), key=lambda x: int(x[0]))

print("Liczba transakcji wg tx_idx:")
for tx_idx, count in sorted_results:
    print(f"{tx_idx}: {count}")

execution_time = time.time() - start_time

with open(os.path.join(base_dir, "data", "times.csv"), "a") as f:
    f.write(f"MapReduce_tx_idx_chunks_with_reduce,{execution_time:.4f}\n")

with open(output_path, mode="w", newline='', encoding="utf-8") as f_out:
    writer = csv.writer(f_out)
    writer.writerow(["tx_idx", "count"])
    writer.writerows(sorted_results)

print(f"\nWyniki zapisane do: {output_path}")
print(f"Czas wykonania: {execution_time:.4f} sekundy")