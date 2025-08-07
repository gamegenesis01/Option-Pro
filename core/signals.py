import yfinance as yf
import pandas as pd
import ta

def generate_trade_ideas(ticker_list):
    trade_ideas = []

    for ticker in ticker_list:
        try:
            df = yf.download(ticker, period="15d", interval="1h")
            if df.empty:
                print(f"[{ticker}] ⚠️ No data returned.")
                continue

            df.dropna(inplace=True)
            df['rsi'] = ta.momentum.RSIIndicator(df['Close'], window=14).rsi()

            latest_rsi = df['rsi'].iloc[-1]

            if latest_rsi < 30:
                direction = "CALL"
            elif latest_rsi > 70:
                direction = "PUT"
            else:
                continue  # No signal

            buy_price = df['Close'].iloc[-1]
            expected_sell = round(buy_price * 1.10, 2) if direction == "CALL" else round(buy_price * 0.90, 2)
            probability = 65 if direction == "CALL" else 60

            trade_ideas.append({
                "ticker": ticker,
                "direction": direction,
                "expiration": "Next Friday",
                "buy_price": round(buy_price, 2),
                "expected_sell": expected_sell,
                "probability": f"{probability}%"
            })

        except Exception as e:
            print(f"[{ticker}] ⚠️ Error: {e}")

    return trade_ideas
