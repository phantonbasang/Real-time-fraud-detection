import json
import joblib
import pandas as pd
import psycopg2
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

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

# Retry connecting to Kafka
print("Connecting to Kafka broker at localhost:29092...")
max_retries = 5
for attempt in range(max_retries):
    try:
        consumer = KafkaConsumer(
            "transactions_stream",
            bootstrap_servers="localhost:29092",
            group_id="fraud_prediction_group_pg_v1",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            request_timeout_ms=30000,
            session_timeout_ms=10000,
            heartbeat_interval_ms=3000,
            api_version_auto_timeout_ms=5000
        )
        print("✅ Connected to Kafka successfully!")
        break
    except NoBrokersAvailable:
        if attempt < max_retries - 1:
            print(f"⚠️  Kafka not ready, retrying in 2 seconds... (attempt {attempt + 1}/{max_retries})")
            time.sleep(2)
        else:
            print("❌ Failed to connect to Kafka after multiple retries")
            print("   Make sure Kafka container is running: docker ps | findstr kafka")
            raise


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
    fraud_proba = float(model.predict_proba(df)[0][1])  # Lấy xác suất fraud
    
    # Validation: Đảm bảo logic nhất quán
    # Nếu prediction = 1 (fraud) thì fraud_proba phải >= 0.5
    # Nếu prediction = 0 (legitimate) thì fraud_proba phải < 0.5
    if pred == 1 and fraud_proba < 0.5:
        print(f"WARNING: Inconsistent prediction - pred={pred} but fraud_proba={fraud_proba:.3f}")
        # Điều chỉnh để nhất quán
        fraud_proba = max(fraud_proba, 0.5)
    elif pred == 0 and fraud_proba >= 0.5:
        print(f"⚠️  WARNING: Inconsistent prediction - pred={pred} but fraud_proba={fraud_proba:.3f}")
        # Điều chỉnh để nhất quán
        fraud_proba = min(fraud_proba, 0.49)

    transaction_id = row.get("id", "")
    client_id = row.get("client_id", "")
    card_id = row.get("card_id", "")

    print(f"Transaction {transaction_id} -> Predicted: {pred} (Fraud Score: {fraud_proba*100:.1f}%)")

    cur.execute(
        """
        INSERT INTO fraud_predictions
        (transaction_id, client_id, card_id, amount, prediction, fraud_probability, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        """,
        (transaction_id, client_id, card_id, amount, pred, fraud_proba)
    )
    conn.commit()

    print("Inserted into PostgreSQL")

    # Gửi đầy đủ thông tin vào Kafka cho Alert System
    producer.send(
        "fraud_predictions",
        {
            "transaction_id": transaction_id,
            "client_id": client_id,
            "card_id": card_id,
            "amount": amount,
            "prediction": pred,
            "fraud_probability": fraud_proba
        }
    )
    print(f"Sent to fraud_predictions topic (Fraud Score: {fraud_proba*100:.1f}%)")
