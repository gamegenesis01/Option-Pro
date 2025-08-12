from __future__ import annotations
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime, time
from typing import Dict, Any

try:
    import pytz
    _TZ = pytz.timezone("US/Eastern")
except Exception:
    _TZ = None


def _now_et() -> datetime:
    now = datetime.utcnow()
    if _TZ:
        return pytz.utc.localize(now).astimezone(_TZ)
    return now


def _is_between(t: time, a: time, b: time) -> bool:
    return a <= t <= b


def _zscore(px: pd.Series, window: int = 20) -> pd.Series:
    """
    Rolling z-score with a scalar guard to avoid
    'truth value of a Series is ambiguous' errors.
    """
    px = px.dropna()
    if len(px) < window + 2:
        return pd.Series(index=px.index, dtype=float)

    mean = px.rolling(window=window).mean()
    sd = px.rolling(window=window).std()

    try:
        last_sd = float(sd.iloc[-1])
    except Exception:
        last_sd = float("nan")

    if last_sd == 0 or np.isnan(last_sd):
        return pd.Series(0.0, index=px.index, dtype=float)

    return (px - mean) / sd


def _hourly_prices(ticker: str, days: int = 15) -> pd.Series:
    df = yf.download(ticker, period=f"{days}d", interval="1h",
                     auto_adjust=True, progress=False)
    if df is None or df.empty or "Close" not in df.columns:
        return pd.Series(dtype=float)
    return df["Close"].dropna()


def _daily_ohlc(ticker: str, days: int = 6) -> pd.DataFrame:
    df = yf.download(ticker, period=f"{days}d", interval="1d",
                     auto_adjust=True, progress=False)
    if df is None or df.empty:
        return pd.DataFrame()
    return df


def forecast_move(ticker: str, horizon_hours: int = 2,
                  bias_mode: str = "revert") -> Dict[str, Any]:
    """
    Lightweight market context used by scoring & filtering.
    """
    ctx: Dict[str, Any] = {
        "S": 0.0,
        "gap_pct": 0.0,
        "mom_1h": 0.0,
        "mom_3h": 0.0,
        "regime_open": 0.0,
        "regime_midday": 0.0,
        "regime_close": 0.0,
        "iv_1d_chg_pts": 0.0,       # placeholder
        "iv_percentile_30d": 0.5,   # placeholder
    }

    px = _hourly_prices(ticker, days=15)
    if px.empty or len(px) < 5:
        return ctx

    ctx["S"] = float(px.iloc[-1])

    # 1h log-return vol (diagnostic)
    try:
        logret = np.log(px / px.shift(1)).dropna().to_numpy()
        ctx["sigma_1h"] = float(np.std(logret, ddof=0))
    except Exception:
        ctx["sigma_1h"] = 0.0

    # short momentum snapshots
    try:
        if len(px) >= 2:
            ctx["mom_1h"] = float((px.iloc[-1] / px.iloc[-2] - 1.0) * 100.0)
        if len(px) >= 4:
            ctx["mom_3h"] = float((px.iloc[-1] / px.iloc[-4] - 1.0) * 100.0)
    except Exception:
        pass

    z20 = _zscore(px, window=20)
    if not z20.empty:
        ctx["zscore_20"] = float(z20.iloc[-1])

    # gap %
    try:
        ddf = _daily_ohlc(ticker, days=6)
        if len(ddf) >= 2:
            today_open = float(ddf["Open"].iloc[-1])
            prior_close = float(ddf["Close"].iloc[-2])
            if prior_close > 0:
                ctx["gap_pct"] = (today_open / prior_close - 1.0) * 100.0
    except Exception:
        pass

    # intraday regime flags (ET)
    try:
        now_et = _now_et().time()
        if _is_between(now_et, time(9, 30), time(11, 0)):
            ctx["regime_open"] = 1.0
        elif _is_between(now_et, time(11, 0), time(15, 30)):
            ctx["regime_midday"] = 1.0
        elif _is_between(now_et, time(15, 30), time(16, 0)):
            ctx["regime_close"] = 1.0
    except Exception:
        pass

    # simple bias toggle
    ctx["bias_mode"] = bias_mode
    if bias_mode == "revert" and "zscore_20" in ctx:
        ctx["bias_value"] = -float(ctx["zscore_20"])
    elif bias_mode == "trend":
        ctx["bias_value"] = float(ctx["mom_3h"])

    return ctx