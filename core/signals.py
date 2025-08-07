import ta

def generate_signals(df):
    # Calculate RSI
    df['rsi'] = ta.momentum.RSIIndicator(df['close'], window=14).rsi()
    latest_rsi = df['rsi'].iloc[-1]

    if latest_rsi < 30:
        return "bullish"
    elif latest_rsi > 70:
        return "bearish"
    else:
        return None
