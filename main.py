import os
import pandas as pd
from datetime import datetime
from core.fetch_data import get_price_data
from core.signals import generate_signal
from core.filter_options import get_best_option
from core.emailer import send_email

TICKERS = [
    "SPY","AAPL","TSLA","MSFT","AMZN","GOOGL",
    "NVDA","META","NFLX","AMD","AAL","PLTR","F","RIVN","SOFI"
]

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def run_bot():
    os.makedirs("output", exist_ok=True)

    log("🔁 Running Option Pro bot...")
    trade_ideas = []

    for ticker in TICKERS:
        try:
            df = get_price_data(ticker, period="2d", interval="5m")
            if df is None or df.empty:
                log(f"[{ticker}] ⚠️ No data.")
                continue

            sig = generate_signal(df)  # returns {"bias": ..., "rsi": ...}
            rsi = sig["rsi"]

            if sig["bias"] is None:
                log(f"[{ticker}] ❌ No signal (RSI {rsi}).")
                continue

            # Map bias -> option direction/type
            trade_stub = {
                "Ticker": ticker,
                "Type": "Call" if sig["bias"] == "bullish" else "Put",
                "Direction": "up" if sig["bias"] == "bullish" else "down",
            }

            idea = get_best_option(ticker, trade_stub)  # your existing (mock) picker
            if idea:
                idea["RSI"] = rsi
                trade_ideas.append(idea)
                log(f"[{ticker}] ✅ {idea['Type']} idea (RSI {rsi}).")
            else:
                log(f"[{ticker}] ⚠️ No option idea produced.")

        except Exception as e:
            log(f"[{ticker}] ⚠️ Error: {e}")

    # Email either way
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if trade_ideas:
        df_out = pd.DataFrame(trade_ideas)
        df_out.to_csv("output/trade_ideas.csv", index=False)
        body = "Here are the latest ideas:\n\n" + "\n".join(
            f"{t['Ticker']} {t['Type']} | Exp {t['Expiration']} | "
            f"Buy ${t['Buy Price']} -> Sell ${t['Sell Price']} | "
            f"Profit ${t['Profit']} | P({t['Probability']}%) | RSI {t.get('RSI','-')}"
            for t in trade_ideas
        )
        send_email(f"Option Pro – {len(trade_ideas)} idea(s) [{ts}]", body)
        log(f"✅ Emailed {len(trade_ideas)} idea(s).")
    else:
        send_email(
            f"Option Pro – No Trade Ideas [{ts}]",
            "Bot ran successfully (5m timeframe) but found no setups within RSI 35/65."
        )
        log("⚠️ No trade ideas this run (email sent).")

if __name__ == "__main__":
    run_bot()