import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List

from config import EMAIL_CONFIG
from logger import get_logger

log = get_logger("emailer")

def send_email(to_addrs: List[str], subject: str, html: str) -> None:
    if not to_addrs:
        log.warning("No recipients for email; skipping.")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_CONFIG["from_addr"]
    msg["To"] = ", ".join(to_addrs)

    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP(EMAIL_CONFIG["smtp_server"], EMAIL_CONFIG["smtp_port"]) as server:
            server.starttls()
            if EMAIL_CONFIG["smtp_password"]:
                server.login(EMAIL_CONFIG["smtp_username"], EMAIL_CONFIG["smtp_password"])
            server.sendmail(msg["From"], to_addrs, msg.as_string())
        log.info(f"Email sent: {subject} -> {to_addrs}")
    except Exception as e:
        log.error(f"Failed sending email: {e}")
