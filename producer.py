import csv
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

CSV_PATH = "data/transactions_data.csv"

print("Sending transactions to Kafka...")

batch = []
batch_size = 10000  # gửi theo lô 10k

with open(CSV_PATH, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)

    for row in reader:
        batch.append(row)

        if len(batch) == batch_size:
            for item in batch:
                producer.send("transactions_stream", item)
            print("Sent batch of 10,000 rows!")
            batch = []

# gửi phần còn lại
for item in batch:
    producer.send("transactions_stream", item)

print("DONE sending all data!")
