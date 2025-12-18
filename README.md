# Real-time Fraud Detection System

Hệ thống phát hiện gian lận giao dịch real-time sử dụng Kafka, XGBoost và Email Alert.

## Kiến trúc hệ thống

```
CSV Data → Producer → Kafka → Consumer (XGBoost) → PostgreSQL
                                    ↓
                              Kafka (fraud_predictions)
                                    ↓
                              Alert System → Email
                                    ↓
                              Dashboard (Flask)
```

## Yêu cầu hệ thống

- Python 3.9+
- Docker Desktop
- Gmail account với App Password

## Cài đặt

### 1. Tạo Virtual Environment

```powershell
python -m venv venv
.\venv\Scripts\activate
```

### 2. Cài đặt thư viện

```powershell
pip install -r requirements.txt
```

### 3. Khởi động Docker Services

```powershell
cd scr
docker-compose up -d
```

**Services:**
- Kafka: `localhost:9092` (internal), `localhost:29092` (external)
- Zookeeper: `localhost:2181`
- PostgreSQL: `localhost:5432`
- PgAdmin: `http://localhost:5050`
- Kafdrop: `http://localhost:19000`

Kiểm tra containers:
```powershell
docker ps
```

## Chạy hệ thống

### Bước 1: Khởi động Alert System (Terminal 1)

```powershell
.\venv\Scripts\activate
python alert/alert_system.py
```

**Cấu hình:**
- File: `alert/alert_system.py` - Dòng 17
- `ENABLE_RATE_LIMITING = True`: Giới hạn 1 email/phút, max 20 emails/giờ
- `ENABLE_RATE_LIMITING = False`: Gửi tất cả emails (dùng khi demo)

### Bước 2: Khởi động Consumer (Terminal 2)

```powershell
.\venv\Scripts\activate
python consumer_predict.py
```

Consumer sẽ:
- Nhận transactions từ Kafka topic `transactions_stream`
- Dự đoán fraud bằng XGBoost model
- Lưu kết quả vào PostgreSQL
- Gửi kết quả đến Kafka topic `fraud_predictions`

### Bước 3: Khởi động Producer (Terminal 3)

```powershell
.\venv\Scripts\activate
python producer.py
```

Producer sẽ đọc file `data/transactions_data.csv` và gửi lên Kafka.

### Bước 4: Khởi động Dashboard (Terminal 4)

```powershell
.\venv\Scripts\activate
cd dashboard
python app.py
```

Truy cập: `http://127.0.0.1:5000`

**Dashboard features:**
- Table View: Xem danh sách transactions
- Charts: Biểu đồ phân tích fraud
- Statistics: Thống kê chi tiết

## Giám sát hệ thống

### Kafdrop (Kafka UI)
- URL: `http://localhost:19000`
- Xem topics, messages, consumer groups

### PgAdmin (PostgreSQL UI)
- URL: `http://localhost:5050`
- Email: `admin@admin.com`
- Password: `admin`

**Kết nối PostgreSQL:**
- Host: `postgres` (trong Docker) hoặc `localhost` (ngoài Docker)
- Port: `5432`
- Database: `fraud_detection`
- Username: `postgres`
- Password: `postgres`

## Email Alert

### Cấu hình
Email được hardcode trong `alert/alert_system.py`:
- Sender: `phantonbasang@gmail.com`
- Recipient: `sangrobo63@gmail.com`
- App Password: `psbv dldu boui ovby`

### Điều kiện gửi email
- Fraud score >= 70% **HOẶC**
- Amount >= $5,000

### Ưu tiên cao (HIGH PRIORITY)
- Fraud score >= 90%
- Bỏ qua rate limiting

## Files quan trọng

### Core System
- `producer.py`: Gửi transactions lên Kafka
- `consumer_predict.py`: Dự đoán fraud và lưu database
- `alert/alert_system.py`: Gửi email cảnh báo
- `dashboard/app.py`: Web dashboard

### Models
- `models/xgb_classifier.joblib`: XGBoost trained model
- `models/feature_columns.json`: 178 features

### Data
- `data/transactions_data.csv`: ~74K transactions
- `data/users_data.csv`: User info
- `data/cards_data.csv`: Card info

### Utilities
- `clean_database.py`: **Xóa database** khi muốn làm lại từ đầu
  - Option 1: DELETE data (giữ structure)
  - Option 2: DROP + CREATE table (reset hoàn toàn)
- `init_postgres.sql`: SQL script tạo table
- `setup.ps1`: Setup script tự động

## Thứ tự chạy chuẩn

1. ✅ Docker services (Kafka, PostgreSQL)
2. ✅ Alert System
3. ✅ Consumer
4. ✅ Producer
5. ✅ Dashboard (optional)

**Lưu ý:** Chạy Alert trước Consumer để không bỏ sót fraud transactions!

## Troubleshooting

### Kafka không kết nối được
```powershell
# Kiểm tra Docker
docker ps

# Restart Kafka
cd scr
docker-compose restart kafka
```

### Consumer lỗi "Invalid file descriptor"
- Tắt consumer cũ trước khi chạy mới
- Chỉ chạy 1 consumer với cùng `group_id`

### Email không gửi
1. Kiểm tra rate limiting (dòng 17 trong alert_system.py)
2. Kiểm tra internet connection
3. Verify Gmail App Password

### Database đầy hoặc muốn reset
```powershell
python clean_database.py
# Chọn option 1 (DELETE) hoặc 2 (DROP+CREATE)
```

### Dashboard không hiển thị dữ liệu
- Kiểm tra PostgreSQL đã có data chưa
- Refresh trang (F5)
- Xem console có lỗi không

## Model Information

**XGBoost Classifier:**
- Features: 178 (từ transactions_data.csv)
- Training accuracy: ~99%
- Fraud rate trong data: ~1-2%

**Fraud threshold:**
- >= 70%: Gửi email alert
- >= 90%: HIGH PRIORITY (bypass rate limit)

## Docker Services Details

### Kafka
- Bootstrap servers: `localhost:9092` (internal), `localhost:29092` (external)
- Topics:
  - `transactions_stream`: Input transactions
  - `fraud_predictions`: Output predictions

### PostgreSQL
- Database: `fraud_detection`
- Table: `fraud_predictions`
- Columns: `transaction_id`, `client_id`, `card_id`, `amount`, `prediction`, `fraud_probability`, `created_at`

## Tắt hệ thống

```powershell
# Tắt Python scripts: Ctrl+C trong mỗi terminal

# Tắt Docker
cd scr
docker-compose down

# Hoặc tắt tất cả và xóa volumes
docker-compose down -v
```

## Notes

- Producer chỉ chạy 1 lần (gửi hết CSV rồi dừng)
- Consumer chạy liên tục (real-time processing)
- Alert system chạy liên tục (monitor fraud_predictions)
- Dashboard có thể chạy/tắt tùy ý

## Contact

- Developer: Your Name
- Email: your.email@example.com
