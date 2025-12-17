CREATE TABLE IF NOT EXISTS fraud_predictions (
    id SERIAL PRIMARY KEY,
    transaction_id TEXT,
    client_id TEXT,
    card_id TEXT,
    amount DOUBLE PRECISION,
    prediction INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
