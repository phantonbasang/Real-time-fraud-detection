"""
Script làm sạch database - Xóa tất cả dữ liệu fraud predictions
Xử lý các lỗi có thể xảy ra và tạo lại bảng nếu cần
"""
import psycopg2
from psycopg2 import sql

def recreate_table(cur, conn):
    """Tạo lại bảng fraud_predictions nếu bị lỗi"""
    print("\n🔧 Đang tạo lại bảng fraud_predictions...")
    
    # Drop bảng cũ nếu tồn tại
    cur.execute("DROP TABLE IF EXISTS fraud_predictions CASCADE")
    
    # Tạo bảng mới
    cur.execute("""
        CREATE TABLE fraud_predictions (
            id SERIAL PRIMARY KEY,
            transaction_id VARCHAR(255) UNIQUE NOT NULL,
            client_id VARCHAR(255),
            card_id VARCHAR(255),
            amount DECIMAL(10, 2),
            prediction INTEGER,
            fraud_probability DECIMAL(5, 4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Tạo index
    cur.execute("CREATE INDEX idx_transaction_id ON fraud_predictions(transaction_id)")
    cur.execute("CREATE INDEX idx_prediction ON fraud_predictions(prediction)")
    cur.execute("CREATE INDEX idx_created_at ON fraud_predictions(created_at)")
    
    conn.commit()
    print("✅ Đã tạo lại bảng thành công!")

def clean_database():
    """Xóa tất cả dữ liệu trong bảng fraud_predictions"""
    
    print("=" * 60)
    print("CLEANING DATABASE - FRAUD PREDICTIONS")
    print("=" * 60)
    
    try:
        # Kết nối PostgreSQL
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="fraud_detection",
            user="postgres",
            password="postgres"
        )
        cur = conn.cursor()
        print("✅ Kết nối PostgreSQL thành công!")
        
        # Kiểm tra bảng có tồn tại không
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'fraud_predictions'
            )
        """)
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            print("\n⚠️  Bảng fraud_predictions không tồn tại!")
            recreate_table(cur, conn)
            return
        
        # Đếm số records hiện tại
        try:
            cur.execute("SELECT COUNT(*) FROM fraud_predictions")
            count_before = cur.fetchone()[0]
            print(f"\n📊 Số records hiện tại: {count_before:,}")
        except Exception as e:
            print(f"\n❌ Lỗi khi đọc dữ liệu: {e}")
            print("🔧 Thử tạo lại bảng...")
            recreate_table(cur, conn)
            return
        
        if count_before == 0:
            print("✅ Database đã sạch, không cần xóa!")
            print("\n💡 Kiểm tra structure bảng...")
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'fraud_predictions'
                ORDER BY ordinal_position
            """)
            columns = cur.fetchall()
            print("\nCột trong bảng:")
            for col in columns:
                print(f"   - {col[0]}: {col[1]}")
            cur.close()
            conn.close()
            return
        
        # Xác nhận xóa
        print(f"\n⚠️  Bạn có chắc muốn XÓA {count_before:,} records?")
        print("Chọn:")
        print("  1. Xóa dữ liệu (DELETE) - Giữ structure")
        print("  2. Tạo lại bảng (DROP + CREATE) - Reset hoàn toàn")
        print("  3. Hủy")
        choice = input("\nNhập lựa chọn (1/2/3): ").strip()
        
        if choice == '1':
            # Xóa dữ liệu
            print("\n🗑️  Đang xóa dữ liệu...")
            try:
                cur.execute("DELETE FROM fraud_predictions")
                conn.commit()
                
                # Reset sequence để ID bắt đầu lại từ 1
                try:
                    cur.execute("ALTER SEQUENCE fraud_predictions_id_seq RESTART WITH 1")
                    conn.commit()
                    print("✅ Đã reset ID sequence")
                except Exception as e:
                    print(f"⚠️  Không reset được sequence: {e}")
            except Exception as e:
                print(f"❌ Lỗi khi xóa: {e}")
                conn.rollback()
                raise
                
        elif choice == '2':
            # Tạo lại bảng
            recreate_table(cur, conn)
        else:
            print("❌ Hủy thao tác")
            cur.close()
            conn.close()
            return
        
        # Kiểm tra lại
        cur.execute("SELECT COUNT(*) FROM fraud_predictions")
        count_after = cur.fetchone()[0]
        
        print("\n" + "=" * 60)
        print("✅ HOÀN TẤT!")
        print("=" * 60)
        print(f"Records trước khi xóa: {count_before:,}")
        print(f"Records sau khi xóa:   {count_after}")
        print("ID sequence đã reset về 1")
        print("=" * 60)
        print("\n💡 Giờ bạn có thể chạy lại Producer + Consumer!")
        print("=" * 60)
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        print("Kiểm tra xem PostgreSQL container có đang chạy không:")
        print("   docker ps | findstr postgres")

if __name__ == "__main__":
    clean_database()
