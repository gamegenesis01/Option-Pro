# core/fetch_data.py

from __future__ import annotations

import math
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

try:
    import yfinance as yf
except Exception as e:  # pragma: no cover
    print(f"[IMPORT][ERROR] Could not import yfinance: {e}", file=sys.stderr)
    raise


# ---------- internal helpers ----------

def _period_from_lookback(lookback_days: int, interval: str) -> str:
    """
    Convert a lookback in *calendar* days to a yfinance 'period' string,
    respecting Yahoo's per-interval limits.

    Yahoo limits (approx):
      - 1m:   up to 7d
      - 2m/5m/15m/30m/60m/90m: up to 60d
      - 1h:   up to 730d (varies)
      - 1d/1wk/1mo: long history supported

    We clamp to safe values to avoid empty frames.
    """
    iv = interval.strip().lower()

    # Minimum 2 days for intraday to ensure we cross non-trading days/weekends.
    min_days = 2 if any(s in iv for s in ("m", "h")) else 1

    # Upper clamps by interval (safe defaults)
    if iv == "1m":
        max_days = 7
    elif any(iv.startswith(x) for x in ("2m", "5m", "15m", "30m", "60m", "90m")):
        max_days = 60
    else:
        # Daily/weekly/monthly — allow larger windows
        max_days = max(lookback_days, 365)

    days = max(min_days, min(int(lookback_days), max_days))
    return f"{days}d"


def _clean_df(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Basic NA cleaning + sanity checks."""
    if df is None or df.empty:
        return None

    # Standardize columns we expect
    expected = {"Open", "High", "Low", "Close", "Adj Close", "Volume"}
    missing = expected.difference(set(df.columns))
    if missing:
        # Some intervals/markets omit "Adj Close" intraday; that’s fine.
        # Only error if we’re missing core OHLC.
        core_missing = {"Open", "High", "Low", "Close"}.intersection(missing)
        if core_missing:
            print(f"[CLEAN][WARN] Missing core columns: {sorted(core_missing)}")
            # Try to recover if 'Adj Close' exists instead of 'Close'
            if "Close" in core_missing and "Adj Close" in df.columns:
                df["Close"] = df["Adj Close"]

    # Drop all-NA rows, then forward/back-fill small gaps
    df = df.dropna(how="all")
    if df.empty:
        return None

    df = df.ffill().bfill()

    # Ensure numeric dtypes for OHLCV where present
    for col in ["Open", "High", "Low", "Close", "Adj Close", "Volume"]:
        if col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            except Exception:
                pass

    df = df.dropna(how="any", subset=[c for c in ["Open", "High", "Low", "Close"] if c in df.columns])
    if df.empty:
        return None

    # Ensure index is tz-aware (UTC) so downstream math doesn’t choke
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, utc=True, errors="coerce")
    else:
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")

    return df


# ---------- public API ----------

def fetch_data(symbol: str, period: str = "5d", interval: str = "15m") -> Optional[pd.DataFrame]:
    """
    Fetch OHLCV data from Yahoo Finance with sensible defaults and robust cleaning.
    Returns a cleaned pandas DataFrame indexed by UTC timestamps, or None on failure.
    """
    symbol = symbol.upper().strip()
    iv = interval.strip().lower()
    per = period.strip().lower()

    print(f"[FETCH] {symbol} period={per} interval={iv}")

    try:
        df = yf.download(
            symbol,
            period=per,
            interval=iv,
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    except Exception as e:
        print(f"[FETCH][ERROR] {symbol} yfinance.download failed: {e}")
        return None

    if df is None or df.empty:
        print(f"[FETCH][WARN] {symbol} returned EMPTY frame (period={per}, interval={iv})")
        return None

    # yfinance sometimes returns a multi-indexed column when multiple tickers are used.
    # Make sure we’re on a single symbol frame.
    if isinstance(df.columns, pd.MultiIndex):
        try:
            df = df.xs(symbol, axis=1, level=0)
        except Exception:
            # Fallback: pick the first level
            df = df.droplevel(0, axis=1)

    print(f"[FETCH][DEBUG] {symbol} raw rows={len(df)} head:\n{df.head(3)}")

    df = _clean_df(df)
    if df is None:
        print(f"[FETCH][WARN] {symbol} frame empty after cleaning")
        return None

    print(f"[FETCH][OK] {symbol} cleaned rows={len(df)} last={df.index[-1].isoformat()}")
    return df


def get_price_history(symbol: str, lookback_days: int = 5, interval: str = "15m") -> Optional[pd.DataFrame]:
    """
    Backwards-compatible wrapper used elsewhere in the codebase:
    accepts lookback_days + interval and converts to a safe 'period' for yfinance.
    """
    try:
        period = _period_from_lookback(int(lookback_days), interval)
    except Exception:
        period = _period_from_lookback(5, interval)

    return fetch_data(symbol=symbol, period=period, interval=interval)


__all__ = ["fetch_data", "get_price_history"]