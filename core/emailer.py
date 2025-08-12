# core/emailer.py
import os, smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_email(subject: str, text_body: str, html_body: str | None = None):
    from_addr = os.getenv("EMAIL_ADDRESS")
    password  = os.getenv("EMAIL_PASSWORD")
    to_addr   = os.getenv("TO_EMAIL")

    if not from_addr or not password or not to_addr:
        print("⚠️ Missing email env vars (EMAIL_ADDRESS / EMAIL_PASSWORD / TO_EMAIL).")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr

    part1 = MIMEText(text_body or "", "plain")
    msg.attach(part1)
    if html_body:
        part2 = MIMEText(html_body, "html")
        msg.attach(part2)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(from_addr, password)
        server.sendmail(from_addr, [to_addr], msg.as_string())