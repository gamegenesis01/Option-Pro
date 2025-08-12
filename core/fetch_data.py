# core/fetch_data.py

from __future__ import annotations

import math
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf


# --- Public helpers ----------------------------------------------------------

def get_price_history(
    symbol: str,
    period_days: int = 30,
    interval: str = "1h",
    *,
    auto_adjust: bool = True,
    min_bars: int = 40,
) -> pd.Series:
    """
    Fetch intraday/daily history from Yahoo Finance and return a clean Close series.

    - Normalizes column names (handles multi-index from yfinance for single symbols)
    - Drops dupes/NaNs, sorts by time, and ensures we have enough bars
    - Returns a float Series named 'Close' indexed by UTC timestamps (if provided)

    Raises:
        ValueError: if no data or insufficient bars are returned.
    """
    if period_days < 1:
        period_days = 1

    period = f"{period_days}d"

    # Defensive download; yfinance changed defaults a few times, we make them explicit.
    try:
        df = yf.download(
            tickers=symbol,
            period=period,
            interval=interval,
            auto_adjust=auto_adjust,
            prepost=False,
            progress=False,
            threads=True,
        )
    except Exception as e:
        raise ValueError(f"{symbol}: download failed ({e})")

    if df is None or df.empty:
        raise ValueError(f"{symbol}: no data returned (period={period}, interval={interval})")

    # yfinance sometimes returns a column MultiIndex even for a single ticker
    if isinstance(df.columns, pd.MultiIndex):
        # Expected shape: top level fields, second level ticker
        # Pick the first (or matching symbol) second-level column
        if symbol in df.columns.get_level_values(-1):
            df = df.xs(symbol, axis=1, level=-1)
        else:
            # Fallback to the first column set
            df = df.droplevel(-1, axis=1)

    # Standardize column capitalization just in case
    cols = {c: c.capitalize() for c in df.columns}
    df = df.rename(columns=cols)

    if "Close" not in df.columns:
        # Sometimes Yahoo returns 'Adj Close' only when auto_adjust=False
        if "Adj Close" in df.columns:
            df["Close"] = df["Adj Close"]
        else:
            raise ValueError(f"{symbol}: Close column missing in response")

    # Clean up
    df = (
        df.sort_index()
          .loc[~df.index.duplicated(keep="last")]
          .dropna(subset=["Close"])
    )

    px = df["Close"].astype(float)

    if px.size < min_bars:
        raise ValueError(
            f"{symbol}: insufficient bars ({px.size} < {min_bars}) "
            f"for period={period}, interval={interval}"
        )

    return px


def compute_log_returns(px: pd.Series) -> pd.Series:
    """
    Log returns from a price series: ln(P_t / P_{t-1})
    """
    if px is None or px.empty:
        raise ValueError("compute_log_returns: empty price series")

    return np.log(px / px.shift(1)).dropna()


def pick_interval_for_window(horizon_hours: int) -> str:
    """
    Choose a reasonable Yahoo interval based on horizon.
    """
    if horizon_hours <= 2:
        return "30m"   # finer granularity helps short horizons
    if horizon_hours <= 6:
        return "1h"
    if horizon_hours <= 48:
        return "2h"
    return "1d"


def get_last_price(symbol: str) -> Optional[float]:
    """
    Best-effort last trade/close price. Returns None if unavailable.
    """
    try:
        tkr = yf.Ticker(symbol)
        # Try fast_info first (quick and usually present)
        fi = getattr(tkr, "fast_info", None)
        if fi and getattr(fi, "last_price", None) is not None:
            return float(fi.last_price)

        # Fallback to 1d/1m history
        h = tkr.history(period="1d", interval="1m", prepost=False)
        if not h.empty and "Close" in h.columns:
            return float(h["Close"].iloc[-1])

        # Fallback to last close
        h = tkr.history(period="2d", interval="1d", prepost=False)
        if not h.empty and "Close" in h.columns:
            return float(h["Close"].iloc[-1])

    except Exception:
        pass

    return None