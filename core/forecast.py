# core/forecast.py
from typing import Literal, Dict, Any
import numpy as np
import pandas as pd
import yfinance as yf

BiasMode = Literal["revert", "best", "none"]

def _zscore(series: pd.Series, window: int = 20) -> float:
    s = series.dropna()
    if len(s) < window + 1:
        return 0.0
    ref = s.tail(window)
    mu = ref.mean()
    sd = ref.std(ddof=0)
    if sd == 0 or np.isnan(sd):
        return 0.0
    return float((s.iloc[-1] - mu) / sd)

def get_hourly_df(ticker: str, period: str = "30d") -> pd.DataFrame:
    """
    Pull hourly bars with auto-adjusted prices.
    """
    df = yf.download(
        ticker, period=period, interval="1h", auto_adjust=True, progress=False
    )
    if df is None or df.empty:
        return pd.DataFrame()
    return df.dropna()

def forecast_move(
    ticker: str,
    horizon_hours: int = 2,
    lookback_hours: int = 72,
    bias_mode: BiasMode = "revert",
) -> Dict[str, Any]:
    """
    Estimate ΔS over a short horizon using realized hourly volatility.
    Returns dict with:
      S                : last close price
      sigma_1h         : realized hourly vol (stdev of log returns)
      dS_mag           : magnitude of move S * sigma_1h * sqrt(H)
      dS_up, dS_dn     : +/− move scenarios
      dt_years         : horizon in years
      zscore_20h       : 20-hour z-score of price
      bias             : 'up'/'down'/None per bias_mode
    bias_mode:
      'revert' : mean-reversion by 20h z-score (z > +1 → down; z < -1 → up)
      'best'   : caller evaluates both directions, pick better later
      'none'   : no bias, just magnitude (bias=None)
    """
    df = get_hourly_df(ticker)
    if df.empty:
        return {
            "S": None, "sigma_1h": None, "dS_mag": None,
            "dS_up": None, "dS_dn": None, "dt_years": None,
            "zscore_20h": None, "bias": None, "ok": False,
            "reason": "no_data"
        }

    # Use last N hours to estimate realized vol
    px = df["Close"].astype(float).dropna()
    if len(px) < max(lookback_hours, 24):
        return {"S": float(px.iloc[-1]), "sigma_1h": None, "dS_mag": None,
                "dS_up": None, "dS_dn": None, "dt_years": horizon_hours / (365.0 * 24.0),
                "zscore_20h": None, "bias": None, "ok": False, "reason": "insufficient_history"}

    S = float(px.iloc[-1])
    logret = np.log(px / px.shift(1)).dropna()
    logret = logret.tail(lookback_hours)
    sigma_1h = float(np.std(logret, ddof=0))  # hourly vol of log-returns

    # Horizon scaling
    H = max(1, int(horizon_hours))
    dS_mag = float(S * sigma_1h * np.sqrt(H))
    dS_up = +dS_mag
    dS_dn = -dS_mag
    dt_years = float(H / (365.0 * 24.0))

    # Bias via mean-reversion on 20h z-score
    z20 = _zscore(px, window=20)
    bias = None
    if bias_mode == "revert":
        if z20 >= +1.0:
            bias = "down"
        elif z20 <= -1.0:
            bias = "up"
        else:
            bias = None
    elif bias_mode == "best":
        bias = None  # evaluate both directions downstream
    else:
        bias = None

    return {
        "S": S,
        "sigma_1h": sigma_1h,
        "dS_mag": dS_mag,
        "dS_up": dS_up,
        "dS_dn": dS_dn,
        "dt_years": dt_years,
        "zscore_20h": z20,
        "bias": bias,
        "ok": True,
        "reason": "ok"
    }
