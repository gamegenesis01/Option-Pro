import os
from core.signals import generate_ranked_ideas
from core.utils import send_email
from datetime import datetime

# Config
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
TO_EMAIL = os.getenv("TO_EMAIL")

# Bot parameters
TICKERS = ["SPY", "AAPL", "TSLA", "MSFT", "AMZN", "GOOGL", "NVDA", "META", "NFLX", "AMD", "AAL", "PLTR", "F", "RIVN", "SOFI"]
HORIZON_HOURS = 2
DELTA_SIGMA = 0.5
DTE_RANGE = (0, 14)
PRICE_RANGE = (-8, 8)
MIN_OI = 100
MAX_SPREAD = 0.35

def run():
    print(f"[{datetime.utcnow()}] 🚀 Running Option Pro (ranked)…")
    
    result = generate_ranked_ideas(
        tickers=TICKERS,
        horizon_hours=HORIZON_HOURS
        # Removed filter_cfg to match old function signature
    )
    
    email_subject = "Option Pro – Ranked Ideas"
    email_body = f"Option Pro – Ranked Ideas\n\n{datetime.utcnow()}\n\n"
    
    if not result:
        email_body += "No trade ideas found."
    else:
        for tier, ideas in result.items():
            email_body += f"{tier}:\n"
            for idea in ideas:
                email_body += f"- {idea}\n"
            email_body += "\n"
    
    send_email(EMAIL_ADDRESS, EMAIL_PASSWORD, TO_EMAIL, email_subject, email_body)

if __name__ == "__main__":
    run()