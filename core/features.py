import pandas as pd
import numpy as np
import ta

def add_features(df: pd.DataFrame, ema_fast: int = 20, ema_slow: int = 50) -> pd.DataFrame:
    """Add RSI, MACD (lines + hist), EMAs, ATR."""
    out = df.copy()

    # Ensure column case
    cols = {c.lower(): c for c in out.columns}
    close = cols.get("close", "Close")
    high  = cols.get("high", "High")
    low   = cols.get("low", "Low")

    # RSI
    out["rsi"] = ta.momentum.RSIIndicator(out[close], window=14).rsi()

    # MACD
    macd = ta.trend.MACD(out[close], window_slow=26, window_fast=12, window_sign=9)
    out["macd"] = macd.macd()
    out["macd_signal"] = macd.macd_signal()
    out["macd_hist"] = macd.macd_diff()

    # EMAs
    out["ema_fast"] = out[close].ewm(span=ema_fast, adjust=False).mean()
    out["ema_slow"] = out[close].ewm(span=ema_slow, adjust=False).mean()
    out["ema_trend"] = out["ema_fast"] - out["ema_slow"]

    # ATR (move gauge)
    atr = ta.volatility.AverageTrueRange(high=out[high], low=out[low], close=out[close], window=14)
    out["atr"] = atr.average_true_range()

    return out

def latest_snapshot(feat_df: pd.DataFrame) -> dict:
    """Return latest feature values as a simple dict."""
    last = feat_df.dropna().iloc[-1]
    return {
        "close": float(last.get("Close", last.get("close", np.nan))),
        "rsi": float(last["rsi"]),
        "macd_hist": float(last["macd_hist"]),
        "ema_trend": float(last["ema_trend"]),
        "atr": float(last["atr"]),
    }