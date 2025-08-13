# core/fetch_data.py
from __future__ import annotations

import io
import math
import time
from datetime import timedelta
from typing import Literal

import numpy as np
import pandas as pd
from urllib.request import urlopen, Request

# Try yfinance first (best coverage). If absent, we’ll use an HTTP CSV fallback.
try:
    import yfinance as yf  # type: ignore
except Exception:
    yf = None  # pragma: no cover

Interval = Literal["1d", "1wk", "1mo", "60m", "1h"]  # we normalize to daily for HTTP fallback


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["close"])
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
    if "close" not in df.columns and "adj_close" in df.columns:
        df["close"] = df["adj_close"]
    keep = [c for c in ("open", "high", "low", "close", "volume") if c in df.columns]
    df = df[keep].copy()
    if "close" in df.columns:
        df["close"] = pd.to_numeric(df["close"], errors="coerce").astype(float)
        df = df[df["close"].notna()]
    # index → UTC DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, utc=True, errors="coerce")
    else:
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")
    return df.sort_index()


def _http_yahoo_csv(symbol: str, lookback_days: int, interval: str = "1d") -> pd.DataFrame:
    """
    Minimal external‑lib‑free fallback using Yahoo CSV download:
    https://query1.finance.yahoo.com/v7/finance/download/{symbol}?...
    Only reliable for daily/weekly/monthly bars; we normalize hourly -> daily.
    """
    interval = "1d" if interval in ("60m", "1h") else interval
    end = int(time.time())
    start = int(time.time() - (lookback_days + 5) * 86400)  # pad a few days
    url = (
        "https://query1.finance.yahoo.com/v7/finance/download/"
        f"{symbol}?period1={start}&period2={end}&interval={interval}"
        "&events=history&includeAdjustedClose=true"
    )
    # Fake a browser UA to avoid 403s
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=15) as resp:
        raw = resp.read()
    df = pd.read_csv(io.BytesIO(raw))
    # Index by Date
    df["Date"] = pd.to_datetime(df["Date"], utc=True, errors="coerce")
    df = df.set_index("Date")
    return _normalize_columns(df)


def get_price_history(symbol: str, lookback_days: int = 60, interval: Interval = "1d") -> pd.DataFrame:
    """
    Fetch OHLCV history with a consistent schema: DatetimeIndex (UTC) and 'close' float column.
    Tries yfinance; if unavailable or empty, uses Yahoo CSV HTTP fallback (daily/weekly/monthly).
    """
    lookback_days = int(lookback_days or 60)
    interval = "60m" if interval == "1h" else interval

    # 1) yfinance route (best if available)
    if yf is not None:
        try:
            period = f"{max(1, lookback_days)}d"
            yf_interval = interval
            raw = yf.Ticker(symbol).history(period=period, interval=yf_interval, auto_adjust=False)
            df = _normalize_columns(raw)
            if not df.empty and np.isfinite(df["close"].iloc[-1]):
                # Trim strictly to window
                cutoff = pd.Timestamp.utcnow().tz_localize("UTC") - pd.Timedelta(days=lookback_days + 1)
                return df[df.index >= cutoff]
        except Exception:
            pass  # fall through to HTTP

    # 2) HTTP CSV fallback (daily/weekly/monthly)
    try:
        df = _http_yahoo_csv(symbol, lookback_days, interval)
        # Final sanity
        if not df.empty and np.isfinite(df["close"].iloc[-1]):
            return df
    except Exception:
        pass

    # 3) Last resort: empty but well‑formed frame
    return pd.DataFrame(columns=["close"]).astype({"close": float})


def latest_close_series(px: pd.DataFrame | pd.Series) -> pd.Series:
    """
    Return a clean 1‑D float Series of closes. Never raises on shape; may be empty.
    """
    if px is None or (hasattr(px, "empty") and px.empty):
        return pd.Series(dtype=float)

    if isinstance(px, pd.Series):
        s = pd.to_numeric(px, errors="coerce").dropna().astype(float)
    else:
        col = "close" if "close" in px.columns else next(
            (c for c in px.columns if pd.api.types.is_numeric_dtype(px[c])), None
        )
        if col is None:
            return pd.Series(dtype=float)
        s = pd.to_numeric(px[col], errors="coerce").dropna().astype(float)

    # Ensure DateTimeIndex if possible
    if not isinstance(s.index, pd.DatetimeIndex):
        try:
            s.index = pd.to_datetime(s.index, utc=True, errors="ignore")
        except Exception:
            pass
    return s.sort_index()