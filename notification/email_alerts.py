import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
TO_EMAIL = os.getenv("TO_EMAIL")

def send_email_alert(trades):
    subject = "📈 Option Pro – New Trade Ideas"
    body = "Here are the latest options trades to consider:\n\n"

    for trade in trades:
        body += (
            f"Ticker: {trade['Ticker']}\n"
            f"Type: {trade['Type']}\n"
            f"Expiration: {trade['Expiration']}\n"
            f"Buy Price: ${trade['Buy Price']:.2f}\n"
            f"Sell Target: ${trade['Sell Price']:.2f}\n"
            f"Profit Potential: ${trade['Profit']:.2f}\n"
            f"Probability of Profit: {trade['Probability']}%\n"
            f"{'-'*30}\n"
        )

    msg = MIMEMultipart()
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = TO_EMAIL
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        server.sendmail(EMAIL_ADDRESS, TO_EMAIL, msg.as_string())
        server.quit()
        print("✅ Email sent.")
    except Exception as e:
        print(f"❌ Email failed to send: {e}")
