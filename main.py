import yfinance as yf
from datetime import datetime
from core.signals import generate_signals
from core.utils import send_email, log

TICKERS = [
    "SPY", "AAPL", "TSLA", "MSFT", "AMZN", "GOOGL",
    "NVDA", "META", "NFLX", "AMD", "AAL", "PLTR",
    "F", "RIVN", "SOFI"
]

log("🔁 Running Option Pro bot...")

trade_ideas = []

for ticker in TICKERS:
    try:
        data = yf.download(ticker, period="7d", interval="15m")
        signal = generate_signals(data)
        if signal:
            trade_ideas.append(f"[{ticker}] {signal}")
    except Exception as e:
        log(f"[{ticker}] ⚠️ Error: {e}")

if not trade_ideas:
    log("⚠️ No trade ideas this hour.")
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    send_email(
        subject=f"Option Pro - No Trade Ideas [{timestamp}]",
        body="The bot ran successfully, but found no trade opportunities this hour."
    )
else:
    message = "📊 Trade Ideas:\n\n" + "\n".join(trade_ideas)
    send_email("Option Pro - Trade Signals", message)
    log("✅ Email with trade ideas sent.")