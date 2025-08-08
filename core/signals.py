import pandas as pd
import ta

def generate_signal(df: pd.DataFrame, rsi_window: int = 14, low: float = 35, high: float = 65):
    """
    Compute RSI and return a signal dict.
    Returns:
      {"bias": "bullish"/"bearish"/None, "rsi": float}
    """
    d = df.copy()
    d["rsi"] = ta.momentum.RSIIndicator(d["Close"], window=rsi_window).rsi()

    rsi_now = float(d["rsi"].iloc[-1])

    if rsi_now <= low:
        return {"bias": "bullish", "rsi": round(rsi_now, 2)}
    if rsi_now >= high:
        return {"bias": "bearish", "rsi": round(rsi_now, 2)}

    return {"bias": None, "rsi": round(rsi_now, 2)}