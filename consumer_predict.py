import json
import joblib
import pandas as pd
import psycopg2
from kafka import KafkaConsumer, KafkaProducer

def safe_float(v):
    if v is None:
        return 0.0
    if isinstance(v, str):
        v = v.replace("$", "").replace(",", "").strip()
        if v == "" or v.lower() == "nan":
            return 0.0
        try:
            return float(v)
        except:
            return 0.0
    try:
        return float(v)
    except:
        return 0.0


model = joblib.load("models/xgb_classifier.joblib")

with open("models/feature_columns.json", "r", encoding="utf-8") as f:
    feature_cols = json.load(f)

print(f"Loaded model with {len(feature_cols)} features")


consumer = KafkaConsumer(
    "transactions_stream",
    bootstrap_servers="localhost:29092",
    group_id="fraud_prediction_group_pg_v1",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)


producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="fraud_detection",
    user="postgres",
    password="postgres"
)
cur = conn.cursor()


print("Consumer started and writing to PostgreSQL")


for msg in consumer:
    row = msg.value

    amount = safe_float(row.get("amount"))
    feature_dict = {col: safe_float(row.get(col, 0)) for col in feature_cols}
    df = pd.DataFrame([feature_dict])

    pred = int(model.predict(df)[0])

    transaction_id = row.get("id", "")
    client_id = row.get("client_id", "")
    card_id = row.get("card_id", "")

    print(f"Transaction {transaction_id} -> Predicted: {pred}")

    cur.execute(
        """
        INSERT INTO fraud_predictions
        (transaction_id, client_id, card_id, amount, prediction)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (transaction_id, client_id, card_id, amount, pred)
    )
    conn.commit()

    print("Inserted into PostgreSQL")

    producer.send(
        "fraud_predictions",
        {
            "transaction_id": transaction_id,
            "prediction": pred
        }
    )
