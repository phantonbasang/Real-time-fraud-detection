import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import sys
import os

# Add parent directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from alert_system import AlertSystem

def test_email_connection():
    """Kiểm tra kết nối email"""
    print("=" * 60)
    print("TESTING EMAIL CONFIGURATION")
    print("=" * 60)
    
    # Load config
    alert_system = AlertSystem(config_file="config/alert_config.json")
    email_config = alert_system.email_config
    
    print(f"\nEmail Configuration:")
    print(f"   Sender: {email_config.get('sender')}")
    print(f"   SMTP Server: {email_config.get('smtp_server')}:{email_config.get('smtp_port')}")
    print(f"   Recipients: {', '.join(email_config.get('recipients', []))}")
    
    # Test SMTP connection
    print(f"\nTesting SMTP connection...")
    try:
        with smtplib.SMTP_SSL(
            email_config.get('smtp_server', 'smtp.gmail.com'),
            email_config.get('smtp_port', 465),
            timeout=10
        ) as server:
            server.login(
                email_config.get('sender'),
                email_config.get('password')
            )
            print("SMTP connection successful!")
            return True
    except Exception as e:
        print(f"SMTP connection failed: {e}")
        return False

def send_test_email():
    """Gửi email test"""
    print("\n" + "=" * 60)
    print("SENDING TEST EMAIL")
    print("=" * 60)
    
    # Tạo transaction data giả với fraud score cao
    test_transaction = {
        'transaction_id': 'TEST-' + datetime.now().strftime('%Y%m%d-%H%M%S'),
        'client_id': 'TEST_CLIENT_001',
        'card_id': 'TEST_CARD_4111111111111111',
        'amount': 7500.50,
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'fraud_score': 0.95  # 95% - Để bypass rate limit
    }
    
    print(f"\nTest Transaction Data:")
    for key, value in test_transaction.items():
        print(f"   {key}: {value}")
    
    # Gửi email
    print(f"\nSending test alert email...")
    alert_system = AlertSystem(config_file=os.path.join(current_dir, "config", "alert_config.json"))
    
    # Force send email (bypass can_send_email check)
    print("Bypassing rate limit for test...")
    success = alert_system.send_email_alert(test_transaction)
    
    if success:
        print("\n" + "=" * 60)
        print("✅ TEST EMAIL SENT SUCCESSFULLY!")
        print("=" * 60)
        print(f"📧 Check your inbox: {', '.join(alert_system.email_config.get('recipients', []))}")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("❌ FAILED TO SEND TEST EMAIL")
        print("=" * 60)
        print("Possible issues:")
        print("1. Check email/password in config/alert_config.json")
        print("2. Make sure 'App Password' is used (not regular Gmail password)")
        print("3. Check internet connection")
        print("=" * 60)
    
    return success

def main():
    """Main test function"""
    print("\nReal-time Fraud Detection - Email Alert System Test")
    print("=" * 60)
    
    # Test 1: SMTP Connection
    if not test_email_connection():
        print("\nCannot proceed with email test - connection failed")
        return
    
    # Test 2: Send test email
    send_test_email()
    
    print("\n" + "=" * 60)
    print("Email Test Complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()
