import yfinance as yf
import pandas as pd
from datetime import datetime
from core.signals import generate_signals
from core.emailer import send_email

TICKERS = ["SPY", "AAPL", "TSLA", "MSFT", "AMZN", "GOOGL", "NVDA", "META", "NFLX", "AMD", "AAL", "PLTR", "F", "RIVN", "SOFI"]

def log(message):
    timestamp = datetime.now().strftime("[%H:%M:%S]")
    print(f"{timestamp} {message}")

def fetch_data(ticker):
    try:
        df = yf.download(ticker, interval="15m", period="1d", progress=False)
        return df
    except Exception as e:
        log(f"[{ticker}] ⚠️ Error fetching data: {e}")
        return None

def run_bot():
    log("🔁 Running Option Pro bot...")

    trade_ideas = []

    for ticker in TICKERS:
        df = fetch_data(ticker)
        if df is None or df.empty:
            log(f"[{ticker}] ⚠️ No data available.")
            continue

        try:
            signal = generate_signals(df)
            if signal:
                trade_ideas.append((ticker, signal))
                log(f"[{ticker}] ✅ Signal: {signal}")
            else:
                log(f"[{ticker}] ❌ No signal.")
        except Exception as e:
            log(f"[{ticker}] ⚠️ Error: {e}")

    if trade_ideas:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        subject = f"Option Pro - Trade Ideas [{timestamp}]"
        body = "\n".join([f"{ticker}: {signal}" for ticker, signal in trade_ideas])
        send_email(subject, body)
    else:
        log("⚠️ No trade ideas this hour.")
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        send_email(
            subject=f"Option Pro - No Trade Ideas [{timestamp}]",
            body="The bot ran successfully, but found no trade opportunities this hour."
        )

if __name__ == "__main__":
    run_bot()