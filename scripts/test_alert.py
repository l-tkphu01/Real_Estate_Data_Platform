import os
os.environ.setdefault("APP_PROFILE", "local")

from src.config import load_settings
from pipelines.hooks import _send_email

def test_alert():
    print("🔄 Đang tải cấu hình Alerts từ .env và config...")
    settings = load_settings()
    
    print("\n[Kiểm Tra Nhanh Thông Số SMTP]")
    print(f"  - SMTP Host: {settings.alerts.smtp_host}")
    print(f"  - SMTP Port: {settings.alerts.smtp_port}")
    print(f"  - Người gửi: {settings.alerts.sender_email}")
    print(f"  - Người nhận: {settings.alerts.recipient_email}")
    
    if not settings.alerts.smtp_password:
        print("  CẢNH BÁO: Không tìm thấy SMTP_PASSWORD trong file .env!")
        return

    print("\n🚀 Bắt đầu gửi email test...")
    try:
        _send_email(
            subject="🔔 [Real Estate Platform] Test Email Thành Công!",
            body="Xin chúc mừng! Nếu bạn nhận được email này, tính năng bắn thông báo Alerts của Dagster đã cấu hình thành công mỹ mãn."
        )
        print("Email đã được bắn đi thành công! Hãy kiểm tra hộp thư của bạn (kể cả mục Spam nhé).")
    except Exception as e:
        print(f"Lỗi gửi email: {e}")
        
if __name__ == "__main__":
    test_alert()
