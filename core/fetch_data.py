# core/fetch_data.py
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Literal, Optional

import numpy as np
import pandas as pd

# yfinance is the most common source used in this project; keep it optional but preferred.
try:
    import yfinance as yf  # type: ignore
except Exception:  # pragma: no cover
    yf = None  # Fallback handled below.


Interval = Literal["1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "1wk", "1mo", "3mo"]


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Make sure price columns are consistently lower‑case and present."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["close"])

    # yfinance returns columns with title case
    df = df.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    )

    # Some sources put price in 'Adj Close' only
    if "close" not in df.columns and "adj_close" in df.columns:
        df["close"] = df["adj_close"]

    # Keep only useful columns; ensure close exists even if empty
    keep = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
    df = df[keep].copy()

    # Drop obvious junk
    if "close" in df.columns:
        df = df[pd.to_numeric(df["close"], errors="coerce").notna()]
        df["close"] = df["close"].astype(float)

    # Make sure the index is tz‑aware UTC and sorted
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, utc=True, errors="coerce")
    else:
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")

    df = df.sort_index()
    return df


def get_price_history(
    symbol: str,
    lookback_days: int = 60,
    interval: Interval = "1d",
) -> pd.DataFrame:
    """
    Fetch OHLCV price history for `symbol` with a predictable schema.

    Returns a DataFrame with at least a float 'close' column and a UTC DateTimeIndex.
    No matter the source quirks, this function *always* normalizes the output so
    callers can safely select `df['close']` (1‑D Series) and pass arrays to indicators.
    """
    # Guard rails
    lookback_days = int(lookback_days or 60)
    interval = str(interval or "1d")

    # If yfinance is available, use it.
    if yf is not None:
        try:
            # yfinance wants period strings for intraday: e.g., "30d" for 30 days
            period = f"{max(1, lookback_days)}d"

            # Map "1h" -> "60m" for yfinance intraday intervals
            yf_interval = "60m" if interval == "1h" else interval

            raw = yf.Ticker(symbol).history(period=period, interval=yf_interval, auto_adjust=False)
            df = _normalize_columns(raw)

            # As a safety buffer, trim strictly to desired window
            cutoff = pd.Timestamp.utcnow().tz_localize("UTC") - pd.Timedelta(days=lookback_days + 1)
            df = df[df.index >= cutoff]

            return df
        except Exception:
            # Fall through to dummy empty frame handled below
            pass

    # Fallback: empty but well‑formed frame (prevents TypeErrors upstream)
    return pd.DataFrame(columns=["close"]).astype({"close": float})


def latest_close_series(
    px: pd.DataFrame | pd.Series,
) -> pd.Series:
    """
    Extract a clean 1‑D float Series of closes from any input (DataFrame/Series).
    This function guarantees a *1‑D* Series so indicators never see 2‑D objects.
    """
    if px is None or (hasattr(px, "empty") and px.empty):
        return pd.Series(dtype=float)

    if isinstance(px, pd.Series):
        s = pd.to_numeric(px, errors="coerce")
        return s.dropna().astype(float)

    # DataFrame path
    if "close" in px.columns:
        s = pd.to_numeric(px["close"], errors="coerce")
    else:
        # Last resort: use first numeric column
        num_cols = [c for c in px.columns if pd.api.types.is_numeric_dtype(px[c])]
        s = pd.to_numeric(px[num_cols[0]], errors="coerce") if num_cols else pd.Series(dtype=float)

    s = s.dropna().astype(float)

    # Ensure DateTimeIndex if possible, otherwise simple RangeIndex is fine
    if not isinstance(s.index, pd.DatetimeIndex):
        try:
            s.index = pd.to_datetime(s.index, utc=True, errors="ignore")
        except Exception:
            pass

    return s