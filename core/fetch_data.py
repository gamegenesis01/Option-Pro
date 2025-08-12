# core/fetch_data.py

from __future__ import annotations

import datetime as _dt
from typing import Optional

import pandas as pd

try:
    import yfinance as yf
except Exception as e:  # pragma: no cover
    yf = None


__all__ = ["get_price_history"]


def _to_naive_utc_index(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure index is tz-naive UTC and sorted."""
    if not isinstance(df.index, pd.DatetimeIndex):
        return df
    idx = df.index
    if idx.tz is not None:
        df.index = idx.tz_convert("UTC").tz_localize(None)
    df = df.sort_index()
    return df


def get_price_history(
    symbol: str,
    lookback_days: int = 60,
    interval: str = "1h",
    start: Optional[_dt.datetime] = None,
    end: Optional[_dt.datetime] = None,
    auto_adjust: bool = True,
    **kwargs,
) -> pd.DataFrame:
    """
    Fetch OHLCV price history for `symbol`.

    Accepts `lookback_days` to be compatible with upstream callers.
    Any extra kwargs are ignored to avoid breaking when callers pass more.

    Parameters
    ----------
    symbol : str
        Ticker, e.g., "AAPL".
    lookback_days : int
        Number of calendar days to fetch when `start`/`end` not provided.
    interval : str
        yfinance interval string, e.g., "1m","5m","15m","1h","1d".
    start, end : datetime | None
        Optional explicit range (UTC). If not given, `lookback_days` is used.
    auto_adjust : bool
        Adjust OHLC to account for splits/dividends.

    Returns
    -------
    pd.DataFrame
        Columns: ["Open","High","Low","Close","Adj Close","Volume"] (where available).
        Index: naive UTC DatetimeIndex.
    """
    if yf is None:
        raise RuntimeError(
            "yfinance is required but not available. Add 'yfinance' to requirements.txt."
        )

    # Determine time window
    if start is None or end is None:
        end = _dt.datetime.utcnow()
        start = end - _dt.timedelta(days=int(lookback_days))

    # yfinance download
    df = yf.download(
        tickers=symbol,
        start=start,
        end=end,
        interval=interval,
        auto_adjust=auto_adjust,
        progress=False,
        threads=False,
    )

    # Some intervals return an empty MultiIndex when no data
    if isinstance(df, pd.DataFrame) and df.empty:
        return df

    # Normalize
    if isinstance(df.columns, pd.MultiIndex):
        # When multiple tickers are passed yfinance creates MultiIndex;
        # we only requested one symbol, so drop the outer level.
        df = df.droplevel(0, axis=1)

    df = _to_naive_utc_index(df)

    # Ensure expected columns exist (fill if missing)
    for col in ["Open", "High", "Low", "Close", "Adj Close", "Volume"]:
        if col not in df.columns:
            if col == "Adj Close" and "Close" in df.columns:
                df["Adj Close"] = df["Close"]
            else:
                df[col] = pd.NA

    return df