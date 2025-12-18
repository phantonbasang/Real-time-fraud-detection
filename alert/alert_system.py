import json
import smtplib
from email.mime.text import MIMEText
from email.mime. multipart import MIMEMultipart
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import psycopg2
from collections import deque
import time

class AlertSystem:
    def __init__(self, config_file="config/alert_config.json"):
        """Khởi tạo alert system"""
        # 🔧 BẬT/TẮT RATE LIMITING: 
        # True = Có giới hạn (1 phút/email, max 20 emails/giờ)
        # False = Không giới hạn (gửi tất cả emails)
        self.ENABLE_RATE_LIMITING = True  # ĐỔI THÀNH False ĐỂ TẮT LIMIT
        
        # Hardcode email config trực tiếp
        self.email_config = {
            "sender": "phantonbasang@gmail.com",
            "password": "psbv dldu boui ovby",
            "recipients": ["sangrobo63@gmail.com"],
            "smtp_server": "smtp.gmail.com",
            "smtp_port": 465,
            "cooldown_minutes": 1,
            "max_emails_per_hour": 20
        }
        
        self.alert_rules = {
            "high_value_threshold": 5000.0,
            "min_fraud_score": 0.70,
            "high_risk_fraud_score": 0.90
        }
        
        self.config = {
            "email": self.email_config,
            "alert_rules": self.alert_rules,
            "enable_email_alerts": True
        }
        
        # Tracking email history để tránh spam
        self.last_email_time = None
        self.email_history = deque(maxlen=100)
        
        # Metrics tracking
        self.metrics = {
            'total_alerts': 0,
            'emails_sent': 0,
            'emails_failed': 0,
            'rate_limited': 0,
            'below_threshold': 0
        }
        
    def get_default_config(self):
        """Default config nếu file không tồn tại"""
        return {
            "email": {
                "sender": "your-email@gmail.com",
                "password": "your-app-password",
                "recipients": ["alert@example.com"],
                "smtp_server": "smtp.gmail. com",
                "smtp_port": 465
            },
            "alert_rules": {
                "high_value_threshold": 5000.0,
                "min_fraud_score": 0.80,
                "high_risk_fraud_score": 0.90
            },
            "enable_email_alerts": True
        }
    
    def send_email_alert(self, transaction_data, retry_count=0, max_retries=3):
        """Gửi email cảnh báo với retry logic"""
        try:
            sender = self.email_config.get('sender')
            password = self.email_config.get('password')
            recipients = self.email_config.get('recipients', [])
            
            if not sender or not password or not recipients:
                print("Email configuration incomplete. Skipping email alert.")
                self.metrics['emails_failed'] += 1
                return False
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"FRAUD ALERT: Transaction #{transaction_data['transaction_id']}"
            msg['From'] = sender
            msg['To'] = ', '.join(recipients)
            
            # HTML email body
            html = f"""
            <html>
              <body style="font-family: Arial, sans-serif;">
                <div style="background-color: #f8d7da; padding: 20px; border-left: 5px solid #dc3545;">
                  <h2 style="color: #dc3545; margin-top: 0;">Fraud Transaction Detected</h2>
                </div>
                
                <div style="padding: 20px;">
                  <table style="border-collapse: collapse; width: 100%;">
                    <tr>
                      <td style="padding:  10px; border: 1px solid #ddd; background-color: #f8f9fa;"><strong>Transaction ID:</strong></td>
                      <td style="padding: 10px; border: 1px solid #ddd;">{transaction_data['transaction_id']}</td>
                    </tr>
                    <tr>
                      <td style="padding: 10px; border: 1px solid #ddd; background-color: #f8f9fa;"><strong>Client ID:</strong></td>
                      <td style="padding: 10px; border: 1px solid #ddd;">{transaction_data['client_id']}</td>
                    </tr>
                    <tr>
                      <td style="padding: 10px; border: 1px solid #ddd; background-color: #f8f9fa;"><strong>Card ID:</strong></td>
                      <td style="padding: 10px; border: 1px solid #ddd;">{transaction_data['card_id']}</td>
                    </tr>
                    <tr>
                      <td style="padding:  10px; border: 1px solid #ddd; background-color: #f8f9fa;"><strong>Amount:</strong></td>
                      <td style="padding: 10px; border: 1px solid #ddd; color: #dc3545; font-weight: bold; font-size: 18px;">${transaction_data['amount']: ,.2f}</td>
                    </tr>
                    <tr>
                      <td style="padding: 10px; border: 1px solid #ddd; background-color: #f8f9fa;"><strong>Timestamp:</strong></td>
                      <td style="padding: 10px; border: 1px solid #ddd;">{transaction_data.get('created_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}</td>
                    </tr>
                    <tr>
                      <td style="padding: 10px; border: 1px solid #ddd; background-color: #f8f9fa;"><strong>Fraud Score:</strong></td>
                      <td style="padding: 10px; border: 1px solid #ddd; color: #dc3545; font-weight: bold;">{transaction_data.get('fraud_score', 0)*100:.1f}%</td>
                    </tr>
                  </table>
                  
                  <div style="margin-top: 20px; padding: 15px; background-color: #fff3cd; border: 1px solid #ffc107; border-radius: 5px;">
                    <strong>Action Required:</strong> Please investigate this transaction immediately.
                  </div>
                </div>
              </body>
            </html>
            """
            
            part = MIMEText(html, 'html')
            msg.attach(part)
            
            # Gửi email qua Gmail SMTP
            with smtplib.SMTP_SSL(
                self.email_config.get('smtp_server', 'smtp.gmail.com'),
                self.email_config.get('smtp_port', 465),
                timeout=30
            ) as server:
                server.login(sender, password)
                server.sendmail(sender, recipients, msg.as_string())
            
            # Lưu thời gian gửi email
            self.last_email_time = datetime.now()
            self.email_history.append({
                'transaction_id': transaction_data['transaction_id'],
                'timestamp': self.last_email_time,
                'amount': transaction_data['amount'],
                'fraud_score': transaction_data.get('fraud_score', 0)
            })
            
            self.metrics['emails_sent'] += 1
            print(f"Email alert sent for transaction {transaction_data['transaction_id']} (Retry: {retry_count})")
            return True
            
        except Exception as e:
            print(f"Failed to send email (attempt {retry_count + 1}/{max_retries}): {e}")
            
            # Retry logic
            if retry_count < max_retries:
                wait_time = 2 ** retry_count  # Exponential backoff: 1s, 2s, 4s
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                return self.send_email_alert(transaction_data, retry_count + 1, max_retries)
            
            self.metrics['emails_failed'] += 1
            print(f"Email failed after {max_retries} retries for transaction {transaction_data['transaction_id']}")
            return False
    
    def can_send_email(self, fraud_score=None, is_high_priority=False):
        """Kiểm tra có thể gửi email không (rate limiting với priority bypass)"""
        # Nếu tắt rate limiting -> gửi tất cả
        if not self.ENABLE_RATE_LIMITING:
            print("⚠️  Rate limiting DISABLED - Sending all emails")
            return True
        
        now = datetime.now()
        
        # HIGH PRIORITY BYPASS: Fraud score >= 90% bỏ qua cooldown
        high_risk_score = self.alert_rules.get('high_risk_fraud_score', 0.90)
        if is_high_priority or (fraud_score and fraud_score >= high_risk_score):
            print(f"HIGH PRIORITY ALERT - Bypassing rate limits (Score: {fraud_score*100 if fraud_score else 'N/A'}%)")
            return True
        
        # Kiểm tra cooldown (thời gian chờ giữa các email)
        cooldown_minutes = self.email_config.get('cooldown_minutes', 5)
        if self.last_email_time:
            time_since_last = (now - self.last_email_time).total_seconds() / 60
            if time_since_last < cooldown_minutes:
                print(f"Email cooldown active. Wait {cooldown_minutes - time_since_last:.1f} more minutes")
                self.metrics['rate_limited'] += 1
                return False
        
        # Kiểm tra số lượng email trong 1 giờ qua
        max_per_hour = self.email_config.get('max_emails_per_hour', 10)
        one_hour_ago = now - timedelta(hours=1)
        recent_emails = [e for e in self.email_history if e['timestamp'] > one_hour_ago]
        
        if len(recent_emails) >= max_per_hour:
            print(f"Email limit reached: {len(recent_emails)}/{max_per_hour} emails sent in last hour")
            self.metrics['rate_limited'] += 1
            return False
        
        return True
    
    def should_alert(self, transaction_data, prediction, fraud_score=None):
        """Kiểm tra xem có nên gửi alert không - Điều kiện độc lập"""
        if prediction != 1:  # Không phải fraud
            return False
        
        # Lấy thresholds từ config
        min_score = self.config.get('min_fraud_score', 0.70)  # 70% từ config
        if fraud_score is not None and fraud_score < min_score:
            print(f"Fraud score {fraud_score*100:.1f}% below threshold {min_score*100:.0f}% - No alert sent")
            self.metrics['below_threshold'] += 1
            return False
        
        amount = float(transaction_data.get('amount', 0))
        high_threshold = self.alert_rules.get('high_value_threshold', 5000.0)
        high_risk_score = self.alert_rules.get('high_risk_fraud_score', 0.90)
        
        # ĐIỀU KIỆN 1: Fraud score >= 90% (HIGH PRIORITY - rất nguy hiểm)
        if fraud_score and fraud_score >= high_risk_score:
            print(f"✅ HIGH RISK: Fraud score {fraud_score*100:.1f}% (>= {high_risk_score*100:.0f}%) - Alert triggered")
            self.metrics['total_alerts'] += 1
            return True
        
        # ĐIỀU KIỆN 2: Fraud score >= 70% (BẤT KỂ amount)
        if fraud_score and fraud_score >= min_score:
            print(f"✅ FRAUD DETECTED: Score {fraud_score*100:.1f}% (>= {min_score*100:.0f}%), Amount ${amount:,.2f} - Alert triggered")
            self.metrics['total_alerts'] += 1
            return True
        
        # ĐIỀU KIỆN 3: Amount >= $5000 (BẤT KỂ fraud score)
        if amount >= high_threshold:
            print(f"✅ HIGH VALUE: Amount ${amount:,.2f} (>= ${high_threshold:,.2f}), Fraud score {fraud_score*100:.1f if fraud_score else 0:.1f}% - Alert triggered")
            self.metrics['total_alerts'] += 1
            return True
        
        return False
    
    def trigger_alerts(self, transaction_data, prediction, fraud_score=None):
        """Kích hoạt alerts"""
        print(f"\n   🔍 Checking alert conditions...")
        
        if not self.should_alert(transaction_data, prediction, fraud_score):
            print(f"   ❌ Alert conditions NOT met")
            return
        
        print(f"   ✅ Alert conditions MET!")
        
        # Xác định priority
        high_risk_score = self.alert_rules.get('high_risk_fraud_score', 0.90)
        is_high_priority = fraud_score and fraud_score >= high_risk_score
        
        # Kiểm tra rate limiting (với priority bypass)
        if not self.can_send_email(fraud_score, is_high_priority):
            print(f"   ⏳ Email rate limit - Alert logged but email not sent for {transaction_data['transaction_id']}")
            return
        
        print(f"\n🚨 TRIGGERING ALERT for transaction {transaction_data['transaction_id']}")
        
        if self.config.get('enable_email_alerts', False):
            # Thêm fraud_score vào transaction_data
            if fraud_score is not None:
                transaction_data['fraud_score'] = fraud_score
            result = self.send_email_alert(transaction_data)
            if result:
                print(f"   ✅ Email sent successfully!")
            else:
                print(f"   ❌ Email sending failed!")
        else:
            print(f"   ⚠️  Email alerts disabled in config")
    
    def print_metrics(self):
        """In ra metrics"""
        print("\n" + "="*60)
        print("ALERT SYSTEM METRICS")
        print("="*60)
        print(f"Total Alerts Triggered:     {self.metrics['total_alerts']}")
        print(f"Emails Sent Successfully: {self.metrics['emails_sent']}")
        print(f"Emails Failed:            {self.metrics['emails_failed']}")
        print(f"Rate Limited:             {self.metrics['rate_limited']}")
        print(f"Below Threshold:          {self.metrics['below_threshold']}")
        
        # Email success rate
        total_attempts = self.metrics['emails_sent'] + self.metrics['emails_failed']
        if total_attempts > 0:
            success_rate = (self.metrics['emails_sent'] / total_attempts) * 100
            print(f"\nEmail Success Rate:       {success_rate:.1f}%")
        
        # Recent email history
        if self.email_history:
            print(f"\nRecent Emails: {len(self.email_history)}")
            for i, email in enumerate(list(self.email_history)[-5:], 1):
                print(f"   {i}. {email['transaction_id']} | ${email['amount']:,.2f} | {email['fraud_score']*100:.1f}%")
        
        print("="*60 + "\n")


def run_alert_consumer():
    """Consumer lắng nghe fraud predictions và gửi alerts"""
    
    print("Starting Alert Consumer...")
    
    # Khởi tạo alert system
    alert_system = AlertSystem()
    
    # Kafka consumer
    consumer = KafkaConsumer(
        "fraud_predictions",
        bootstrap_servers="localhost:29092",
        group_id="alert_consumer_group_v2",  # Đổi v2 để đọc lại từ đầu
        value_deserializer=lambda m: json. loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )
    
    # Database connection
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="fraud_detection",
        user="postgres",
        password="postgres"
    )
    
    # Counter để in metrics định kỳ
    message_count = 0
    
    try:
        print("\n🎯 Alert Consumer is listening for fraud predictions...")
        print("=" * 60)
        
        for msg in consumer:
            data = msg.value
            message_count += 1
            
            transaction_id = data.get('transaction_id')
            prediction = data.get('prediction')
            fraud_probability = data.get('fraud_probability', 0.0)  # Lấy fraud score
            
            print(f"\n📨 Received message #{message_count}: {transaction_id}")
            print(f"   Prediction: {prediction}, Fraud Score: {fraud_probability*100:.1f}%")
            
            if prediction == 1:  # Fraud detected
                # Sử dụng data từ Kafka trực tiếp (đã có đầy đủ info)
                client_id = data.get('client_id', 'UNKNOWN')
                card_id = data.get('card_id', 'UNKNOWN')
                amount = float(data.get('amount', 0))
                fraud_score = fraud_probability
                
                transaction_data = {
                    'transaction_id': transaction_id,
                    'client_id': client_id,
                    'card_id': card_id,
                    'amount': amount,
                    'created_at': None  # Sẽ dùng thời gian hiện tại trong email
                }
                
                print(f"   Client: {client_id}, Card: {card_id}")
                print(f"   💰 Amount: ${amount:,.2f}")
                print(f"   🎯 Processing alert check...")
                
                alert_system.trigger_alerts(transaction_data, prediction, fraud_score)
            
            # In metrics mỗi 50 messages
            if message_count % 50 == 0:
                alert_system.print_metrics()
                    
    except KeyboardInterrupt:
        print("\n\nStopping alert consumer...")
        alert_system.print_metrics()
    finally:
        consumer.close()
        conn.close()
        print("Alert consumer stopped")


if __name__ == "__main__":
    run_alert_consumer()