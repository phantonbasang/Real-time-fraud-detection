CREATE TABLE IF NOT EXISTS fraud_predictions (
    id SERIAL PRIMARY KEY,
    transaction_id TEXT,
    client_id TEXT,
    card_id TEXT,
    amount DOUBLE PRECISION,
    prediction INTEGER,
    fraud_probability DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Ho_Chi_Minh')
);
