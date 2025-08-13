# core/fetch_data.py
# Fetches intraday price history and returns a clean DataFrame with
# columns: ['datetime','open','high','low','close','volume']
# - Normalizes provider column names
# - Ensures numeric types
# - Drops the most-recent incomplete bar (missing/NaN close)
# - Filters to the last `hours` of data

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

# yfinance is the default data source; it’s commonly available on GH runners.
# If you’re using another source, you can adapt the _fetch_* function below.
try:
    import yfinance as yf
    _HAS_YFINANCE = True
except Exception:
    _HAS_YFINANCE = False


# ---------- Public API ----------

def get_price_history(
    symbol: str,
    *,
    interval: str = "5m",
    hours: int = 8,
    source: str = "yfinance",
    drop_incomplete_last: bool = True,
) -> pd.DataFrame:
    """
    Fetch normalized intraday OHLCV for `symbol`.

    Returns a DataFrame with:
        ['datetime','open','high','low','close','volume']  (all lowercase)
    Datetimes are timezone-aware in UTC, sorted ascending.
    """
    if hours <= 0:
        hours = 8

    if source.lower() == "yfinance":
        if not _HAS_YFINANCE:
            raise RuntimeError("yfinance not available but source='yfinance' requested.")
        raw = _fetch_yfinance(symbol, interval=interval, hours=hours)
    else:
        raise ValueError(f"Unsupported source '{source}'. Only 'yfinance' is implemented.")

    df = _normalize_ohlcv(raw)

    # keep just the last `hours` of data
    now_utc = datetime.now(timezone.utc)
    earliest = now_utc - timedelta(hours=hours)
    if "datetime" in df.columns:
        df = df[df["datetime"] >= earliest]

    # drop any rows without a numeric close
    df = df[pd.to_numeric(df["close"], errors="coerce").notna()]

    # drop most recent row if it's incomplete or NaN close
    if drop_incomplete_last and not df.empty:
        df = _drop_incomplete_last_bar(df, interval)

    if df.empty or df["close"].isna().all():
        # Let the caller decide what to do; downstream code will surface as 'bad_snapshot'
        raise ValueError(f"No valid close found for {symbol} (interval={interval}, hours={hours}).")

    return df.reset_index(drop=True)


# ---------- Provider fetchers ----------

def _fetch_yfinance(symbol: str, *, interval: str, hours: int) -> pd.DataFrame:
    """
    Use yfinance to pull intraday bars. yfinance limits the allowed period by interval.
    We choose a safe period that covers the requested `hours`.
    """
    interval = _normalize_interval(interval)

    # period mapping based on yfinance rules
    # ref: yfinance intraday periods; we pick something roomy to ensure enough bars
    needed_hours = max(1, hours)
    if interval in {"1m"}:
        period = "7d"          # 1m bars need period <= 7d
    elif interval in {"2m", "5m", "15m"}:
        period = "30d"         # safe for these intervals
    elif interval in {"30m", "60m", "90m", "1h"}:
        period = "60d"
    else:
        # for daily or anything else, ask for 90d
        period = "90d"

    # Fetch
    # auto_adjust=False -> we get raw OHLCV; we’ll just use Close as-is
    df = yf.download(
        tickers=symbol,
        interval=interval,
        period=period,
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    if df is None or len(df) == 0:
        return pd.DataFrame()

    # yfinance may return a MultiIndex on columns for some tickers
    if isinstance(df.columns, pd.MultiIndex):
        # Typical columns look like [('Open', ''), ('High',''), ...]
        df.columns = [c[0] for c in df.columns]

    df = df.copy()

    # Ensure DatetimeIndex -> column
    if isinstance(df.index, pd.DatetimeIndex):
        # yfinance returns exchange tz; convert to UTC and make tz-aware
        idx = df.index.tz_convert("UTC") if df.index.tz is not None else df.index.tz_localize("UTC")
        df.insert(0, "datetime", idx.to_pydatetime())
        df.reset_index(drop=True, inplace=True)
    else:
        # Fallback: create a naive datetime column if something odd happens
        df.insert(0, "datetime", pd.to_datetime(df.index, utc=True))
        df.reset_index(drop=True, inplace=True)

    return df


# ---------- Normalization & helpers ----------

_COLMAP = {
    # common variants -> our canonical lowercase
    "datetime": "datetime",
    "date": "datetime",
    "time": "datetime",
    "timestamp": "datetime",

    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "adj close": "close",
    "adj_close": "close",
    "last": "close",
    "price": "close",

    "volume": "volume",
    "vol": "volume",
}

def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])

    # Lowercase columns first
    cols_lower = {c: str(c).strip().lower() for c in df.columns}
    df = df.rename(columns=cols_lower)

    # Map to canonical names
    remapped = {}
    for c in df.columns:
        remapped[c] = _COLMAP.get(c, c)
    df = df.rename(columns=remapped)

    # If we still don't have the essentials, try to infer
    needed = {"datetime", "open", "high", "low", "close", "volume"}
    for need in needed:
        if need not in df.columns:
            if need == "volume" and "shares" in df.columns:
                df = df.rename(columns={"shares": "volume"})
            elif need == "datetime" and isinstance(df.index, pd.DatetimeIndex):
                # make an explicit datetime column
                idx = df.index
                idx = idx.tz_convert("UTC") if idx.tz is not None else idx.tz_localize("UTC")
                df.insert(0, "datetime", idx.to_pydatetime())
            else:
                # create missing with NaN to keep schema
                df[need] = pd.NA

    # Cast dtypes
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Coerce datetime column to UTC tz-aware
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")

    # Keep only the canonical columns in order
    df = df[["datetime", "open", "high", "low", "close", "volume"]].dropna(subset=["datetime"])

    # Sort ascending time
    df = df.sort_values("datetime")

    return df


def _normalize_interval(interval: str) -> str:
    """Normalize common interval aliases (e.g., '1h' -> '60m')."""
    s = interval.strip().lower()
    if s in {"60m", "1h"}:
        return "60m"
    if s in {"30m"}:
        return "30m"
    if s in {"15m"}:
        return "15m"
    if s in {"5m"}:
        return "5m"
    if s in {"1m"}:
        return "1m"
    if s in {"90m", "1.5h"}:
        return "90m"
    if s in {"1d", "d", "day", "daily"}:
        return "1d"
    return s  # pass through


def _interval_to_timedelta(interval: str) -> Optional[timedelta]:
    s = _normalize_interval(interval)
    if s.endswith("m"):
        try:
            minutes = int(s[:-1])
            return timedelta(minutes=minutes)
        except Exception:
            return None
    if s.endswith("d"):
        try:
            days = int(s[:-1])
            return timedelta(days=days)
        except Exception:
            return None
    return None


def _drop_incomplete_last_bar(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    """
    Remove the last row if it looks incomplete (NaN close) or
    if its timestamp is within the current interval window (i.e., candle still forming).
    """
    if df.empty:
        return df

    df = df.copy()
    last_idx = df.index[-1]
    last_row = df.loc[last_idx]

    # If close is NaN -> drop it
    if pd.isna(last_row.get("close")):
        return df.iloc[:-1]

    # If candle still forming (timestamp within the current interval window), drop it
    step = _interval_to_timedelta(interval)
    if step is not None:
        now_utc = datetime.now(timezone.utc)
        last_dt: datetime = last_row["datetime"]
        # if last_dt is too close to now (less than interval), drop it
        if (now_utc - last_dt) < step:
            return df.iloc[:-1]

    return df