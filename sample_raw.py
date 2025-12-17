import csv
import json

CSV_PATH = "data/transactions_data.csv"
OUTPUT_PATH = "sample_raw_transactions.json"

sample_size = 20
samples = []

with open(CSV_PATH, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)

    for i, row in enumerate(reader):
        samples.append(row)
        if i + 1 >= sample_size:
            break

with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(samples, f, indent=4, ensure_ascii=False)

print(f"Created sample raw file: {OUTPUT_PATH}")
