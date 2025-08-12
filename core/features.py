# core/features.py
from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Dict, Any


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add technical features to the price history DataFrame.
    Expects columns: ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    """
    df = df.copy()
    if "Close" not in df.columns:
        return df

    df["returns"] = df["Close"].pct_change()
    df["volatility"] = df["returns"].rolling(window=20).std()
    df["sma_10"] = df["Close"].rolling(window=10).mean()
    df["sma_50"] = df["Close"].rolling(window=50).mean()
    df["rsi_14"] = compute_rsi(df["Close"], 14)
    return df


def latest_snapshot(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Return the latest snapshot as a dictionary.
    """
    if df is None or len(df) == 0:
        return {}
    last = df.iloc[-1]
    return {
        "price": float(last["Close"]),
        "date": last["Date"] if "Date" in last else None,
    }


def exp_dS(df: pd.DataFrame, horizon_hours: float = 2, bias_mode: str = "revert") -> float:
    """
    Expected price change in dollars over horizon_hours based on recent volatility.
    bias_mode: 'revert' biases towards mean reversion, 'trend' biases towards continuation.
    """
    if df is None or len(df) < 2:
        return 0.0

    # use log returns
    df = df.copy()
    df["log_ret"] = np.log(df["Close"] / df["Close"].shift(1))
    hourly_vol = df["log_ret"].std() * np.sqrt(1)  # per hour if data is hourly
    mean_price = df["Close"].iloc[-1]

    exp_move = hourly_vol * mean_price * (horizon_hours ** 0.5)

    if bias_mode == "revert":
        exp_move *= -1 if df["Close"].iloc[-1] > df["Close"].mean() else 1
    elif bias_mode == "trend":
        exp_move *= 1 if df["Close"].iloc[-1] > df["Close"].mean() else -1

    return round(exp_move, 3)


def exp_dIV(df: pd.DataFrame) -> float:
    """
    Expected implied volatility change in percentage points.
    Here: approximated by recent volatility of returns * scaling factor.
    """
    if df is None or len(df) < 2:
        return 0.0

    df = df.copy()
    df["log_ret"] = np.log(df["Close"] / df["Close"].shift(1))
    vol_points = df["log_ret"].std() * 100 * 0.5  # scale factor 0.5 for realism
    return round(vol_points, 3)


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


__all__ = ["add_features", "latest_snapshot", "exp_dS", "exp_dIV"]