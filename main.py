import pandas as pd
import time
from datetime import datetime
from core.fetch_data import get_price_data
from core.signals import generate_signals
from core.strategy import find_trade_opportunity
from core.filter_options import get_best_option
from notification.email_alerts import send_email_alert

def run_bot():
    try:
        with open("data/symbols.txt", "r") as file:
            tickers = [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        print("❌ symbols.txt not found. Please add some tickers in /data/symbols.txt")
        return

    trade_ideas = []

    for ticker in tickers:
        try:
            df = get_price_data(ticker)
            signal = generate_signals(df)

            if signal:
                trade = find_trade_opportunity(ticker, signal, df)
                option = get_best_option(ticker, trade)
                if option:
                    trade_ideas.append(option)
        except Exception as e:
            print(f"[{ticker}] ⚠️ Error: {e}")

    if trade_ideas:
        df = pd.DataFrame(trade_ideas)
        df.to_csv("output/trade_ideas.csv", index=False)

        print(f"📧 Found {len(trade_ideas)} trades. Preparing email...")
        send_email_alert(trade_ideas)
        print("✅ Email function completed.")
    else:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ No trade ideas this hour.")

if __name__ == "__main__":
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔁 Running Option Pro bot...")
    run_bot()
