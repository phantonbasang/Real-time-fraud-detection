from flask import Flask, render_template, request
import psycopg2

app = Flask(__name__)

def get_conn():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="fraud_detection",
        user="postgres",
        password="postgres"
    )

@app.route("/")
def dashboard():
    view = request.args.get("view", "table")

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM fraud_predictions")
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM fraud_predictions WHERE prediction = 1")
    fraud = cur.fetchone()[0]

    fraud_rate = round((fraud / total) * 100, 2) if total > 0 else 0

    if view == "chart":
        cur.execute("""
            SELECT prediction, COUNT(*)
            FROM fraud_predictions
            GROUP BY prediction
            ORDER BY prediction
        """)
        data = cur.fetchall()
        cur.close()
        conn.close()

        label_map = {
        0: "Not Fraud",
        1: "Fraud"
        }

        labels = []
        values = []

        for p, c in data:
            labels.append(label_map.get(p, str(p)))
            values.append(c)

        return render_template(
            "index.html",
            view=view,
            total=total,
            fraud=fraud,
            fraud_rate=fraud_rate,
            labels=labels,
            values=values
        )

    cur.execute("""
        SELECT transaction_id, client_id, card_id, amount, prediction, created_at
        FROM fraud_predictions
        ORDER BY id DESC
        LIMIT 50
    """)
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return render_template(
        "index.html",
        view=view,
        total=total,
        fraud=fraud,
        fraud_rate=fraud_rate,
        rows=rows
    )

if __name__ == "__main__":
    app.run(debug=True, port=5000)
