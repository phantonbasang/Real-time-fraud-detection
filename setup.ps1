Write-Host "=== TẠO MÔI TRƯỜNG ẢO ==="
python -m venv .venv

Write-Host "=== KÍCH HOẠT MÔI TRƯỜNG ẢO ==="
.\.venv\Scripts\Activate.ps1

Write-Host "=== CÀI ĐẶT THƯ VIỆN CẦN THIẾT ==="

pip install --upgrade pip

pip install kafka-python
pip install joblib
pip install numpy
pip install pandas
pip install xgboost
pip install scikit-learn
pip install streamlit
pip install flask
pip install psycopg2-binary
pip install flask psycopg2-binary

Write-Host "=== HOÀN TẤT ==="
Write-Host "Môi trường ảo đã sẵn sàng. Bạn có thể chạy project!"
