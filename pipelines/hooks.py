import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dagster import HookContext, success_hook, failure_hook
from src.config import load_settings

def _send_email(subject: str, body: str, context: HookContext) -> bool:
    """Tiện ích gửi email ẩn danh qua SMTP dựa trên config hiện hành."""
    settings = load_settings()
    if not settings.alerts.email_enabled:
        return False
        
    sender = settings.alerts.sender_email
    recipient = settings.alerts.recipient_email
    password = settings.alerts.smtp_password
    
    if not password or password == "dien_mat_khau_ung_dung_vao_day":
        context.log.warning("⚠️ [Hooks Bỏ Qua]: Không tìm thấy mật khẩu SMTP hợp lệ trong .env. Đã hủy lệnh gửi Mail.")
        return False
        
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = recipient
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain', 'utf-8'))
    
    try:
        server = smtplib.SMTP(settings.alerts.smtp_host, settings.alerts.smtp_port)
        server.starttls()
        server.login(sender, password)
        server.send_message(msg)
        server.quit()
        return True
    except Exception as e:
        context.log.error(f"❌ [Lỗi Hệ Thống SMTP Gửi Email]: {e}")
        return False

@success_hook
def step_success_alert(context: HookContext):
    """Gửi email khi một Bước (Op) báo cáo hoàn thành phi thường."""
    op_name = context.op.name
    subject = f"✅ [DAGSTER THÀNH CÔNG] - Bước: {op_name} đã vượt ải"
    body = (
        f"Hoàn thành xuất sắc bước: {op_name}\n"
        f"Thời gian báo cáo: Hãy kiểm tra Dagster UI để xem chi tiết log thời gian.\n\n"
        f"Hệ thống tự động: Real Estate Data Platform"
    )
    if _send_email(subject, body, context):
        context.log.info(f"Đã gửi email thông báo THÀNH CÔNG cho {op_name}")

@failure_hook
def step_failure_alert(context: HookContext):
    """Gửi báo động đỏ cấp 1 khi một Bước (Op) toang dọc đường."""
    op_name = context.op.name
    error_msg = str(context.op_exception) if context.op_exception else "Lỗi không xác định."
    
    subject = f"❌ [DAGSTER THẤT BẠI] - Báo Động Đỏ Bước: {op_name}"
    body = (
        f"Đã xảy ra lỗi nghiêm trọng quật ngã bước: {op_name}!\n\n"
        f"Vui lòng đăng nhập Dagster để kiểm tra Stack Trace ngay.\n\n"
        f"Mô tả lỗi rút gọn:\n{error_msg}\n\n"
        f"Hệ thống cảnh báo: Real Estate Data Platform"
    )
    if _send_email(subject, body, context):
        context.log.info(f"Đã gửi email báo THẤT BẠI cho {op_name}")
