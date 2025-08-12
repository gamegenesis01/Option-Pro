# core/features.py

from __future__ import annotations

import math
from typing import Dict, Optional

import numpy as np
import pandas as pd


__all__ = ["add_features", "latest_snapshot", "exp_dS", "exp_dIV"]


def _to_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def add_features(px: pd.DataFrame) -> pd.DataFrame:
    """
    Very light feature set; safe on intraday/daily.
    Adds: return_1, return_5 (pct returns).
    """
    if px is None or len(px) == 0:
        return px

    df = px.copy()
    df = _to_numeric(df, ["Open", "High", "Low", "Close", "Adj Close", "Volume"])
    # prefer Close; fall back to Adj Close
    price = df["Close"].where(~df["Close"].isna(), df.get("Adj Close"))
    df["return_1"] = price.pct_change(1)
    df["return_5"] = price.pct_change(5)
    return df


def latest_snapshot(px: pd.DataFrame) -> Optional[Dict]:
    """
    Pull a robust “last tick” snapshot from a price frame.
    Returns None if a valid numeric last price isn’t available.
    """
    if px is None or len(px) == 0:
        return None

    df = px.copy()
    df = _to_numeric(df, ["Open", "High", "Low", "Close", "Adj Close", "Volume"])
    df = df.sort_index()

    # Forward/back fill a little to reduce transient NAs from yfinance gaps.
    df = df.ffill().bfill()

    last = df.iloc[-1]

    price = last.get("Close")
    if pd.isna(price) and "Adj Close" in df.columns:
        price = last.get("Adj Close")

    if price is None or (isinstance(price, float) and (math.isnan(price))):
        return None

    try:
        price_f = float(price)
    except Exception:
        return None

    vol = last.get("Volume", 0.0)
    try:
        vol_f = float(vol) if not pd.isna(vol) else 0.0
    except Exception:
        vol_f = 0.0

    return {
        "ts": df.index[-1],
        "price": price_f,
        "volume": vol_f,
    }


def exp_dS(horizon_hours: int, daily_vol_pct: float) -> float:
    """
    Very simple expected move proxy (percent of price) over horizon.
    daily_vol_pct is e.g. 2.0 for 2%.
    """
    # scale daily vol to hours assuming sqrt time
    return max(0.0, float(daily_vol_pct)) * math.sqrt(max(horizon_hours, 0) / 24.0)


def exp_dIV(points: float = 0.5) -> float:
    """Expected IV shift in absolute points (e.g., 0.5)."""
    return float(points)