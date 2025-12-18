from flask import Flask, render_template, request, jsonify
import psycopg2
from datetime import datetime, timedelta

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
    # Tab and filter parameters
    tab = request.args.get('tab', 'table')
    fraud_only = request.args.get('fraud_only', 'false')
    
    # Pagination parameters
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    offset = (page - 1) * per_page
    
    conn = get_conn()
    cur = conn.cursor()

    # Basic stats
    cur.execute("SELECT COUNT(*) FROM fraud_predictions")
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM fraud_predictions WHERE prediction = 1")
    fraud = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM fraud_predictions WHERE prediction = 0")
    legitimate = cur.fetchone()[0]

    fraud_rate = round((fraud / total) * 100, 2) if total > 0 else 0

    # Total amount stats
    cur.execute("SELECT COALESCE(SUM(amount), 0) FROM fraud_predictions WHERE prediction = 1")
    fraud_amount = round(cur.fetchone()[0], 2)

    cur.execute("SELECT COALESCE(AVG(amount), 0) FROM fraud_predictions WHERE prediction = 1")
    avg_fraud_amount = round(cur.fetchone()[0], 2)

    # High risk transactions (fraud_probability >= 90%)
    cur.execute("SELECT COUNT(*) FROM fraud_predictions WHERE fraud_probability >= 0.90")
    high_risk = cur.fetchone()[0]

    # Recent 24h stats
    cur.execute("""
        SELECT COUNT(*) FROM fraud_predictions 
        WHERE created_at >= NOW() - INTERVAL '24 hours' AND prediction = 1
    """)
    fraud_24h = cur.fetchone()[0]

    # Fraud distribution by prediction
    cur.execute("""
        SELECT prediction, COUNT(*)
        FROM fraud_predictions
        GROUP BY prediction
        ORDER BY prediction
    """)
    prediction_data = cur.fetchall()

    # Recent transactions with pagination and filter
    if fraud_only == 'true':
        cur.execute("SELECT COUNT(*) FROM fraud_predictions WHERE prediction = 1")
        filtered_total = cur.fetchone()[0]
        
        cur.execute("""
            SELECT transaction_id, client_id, card_id, amount, prediction, 
                   fraud_probability, created_at
            FROM fraud_predictions
            WHERE prediction = 1
            ORDER BY id DESC
            LIMIT %s OFFSET %s
        """, (per_page, offset))
        recent_transactions = cur.fetchall()
        
        # Calculate total pages for filtered view
        total_pages = (filtered_total + per_page - 1) // per_page
    else:
        cur.execute("""
            SELECT transaction_id, client_id, card_id, amount, prediction, 
                   fraud_probability, created_at
            FROM fraud_predictions
            ORDER BY id DESC
            LIMIT %s OFFSET %s
        """, (per_page, offset))
        recent_transactions = cur.fetchall()
        
        # Calculate total pages
        total_pages = (total + per_page - 1) // per_page
        filtered_total = total
    
    # Calculate showing range
    showing_start = (page - 1) * per_page + 1
    showing_end = min(page * per_page, filtered_total)
    
    # Calculate pagination range
    start_page = max(1, page - 2)
    end_page = min(total_pages, page + 2)

    # Recent fraud transactions
    cur.execute("""
        SELECT transaction_id, client_id, amount, fraud_probability, created_at
        FROM fraud_predictions
        WHERE prediction = 1
        ORDER BY id DESC
        LIMIT 5
    """)
    recent_frauds = cur.fetchall()

    # Hourly fraud trend (all data)
    cur.execute("""
        SELECT 
            DATE_TRUNC('hour', created_at) as hour,
            COUNT(*) as total_count,
            SUM(CASE WHEN prediction = 1 THEN 1 ELSE 0 END) as fraud_count
        FROM fraud_predictions
        GROUP BY hour
        ORDER BY hour
    """)
    hourly_trend = cur.fetchall()

    cur.close()
    conn.close()

    # Prepare chart data
    label_map = {0: "Legitimate", 1: "Fraud"}
    pred_labels = [label_map.get(p, str(p)) for p, c in prediction_data]
    pred_values = [c for p, c in prediction_data]

    # Hourly trend data
    trend_hours = [t[0].strftime('%H:%M') if t[0] else '' for t in hourly_trend]
    trend_totals = [t[1] for t in hourly_trend]
    trend_frauds = [t[2] for t in hourly_trend]

    return render_template(
        "index.html",
        tab=tab,
        fraud_only=fraud_only,
        total=total,
        fraud=fraud,
        legitimate=legitimate,
        fraud_rate=fraud_rate,
        fraud_amount=fraud_amount,
        avg_fraud_amount=avg_fraud_amount,
        high_risk=high_risk,
        fraud_24h=fraud_24h,
        pred_labels=pred_labels,
        pred_values=pred_values,
        recent_transactions=recent_transactions,
        recent_frauds=recent_frauds,
        trend_hours=trend_hours,
        trend_totals=trend_totals,
        trend_frauds=trend_frauds,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        showing_start=showing_start,
        showing_end=showing_end,
        start_page=start_page,
        end_page=end_page,
        filtered_total=filtered_total
    )

@app.route("/api/stats")
def api_stats():
    """API endpoint for real-time stats update"""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM fraud_predictions")
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM fraud_predictions WHERE prediction = 1")
    fraud = cur.fetchone()[0]

    fraud_rate = round((fraud / total) * 100, 2) if total > 0 else 0

    cur.execute("""
        SELECT COUNT(*) FROM fraud_predictions 
        WHERE created_at >= NOW() - INTERVAL '1 minute' AND prediction = 1
    """)
    fraud_last_min = cur.fetchone()[0]

    cur.close()
    conn.close()

    return jsonify({
        'total': total,
        'fraud': fraud,
        'fraud_rate': fraud_rate,
        'fraud_last_min': fraud_last_min
    })

if __name__ == "__main__":
    app.run(debug=True, port=5000)
