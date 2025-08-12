# core/features.py

from __future__ import annotations

import math
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


# ----------------------------
# Helpers (pure pandas/numpy)
# ----------------------------

def _rsi(close: pd.Series, window: int = 14) -> pd.Series:
    """Vanilla RSI implementation without external libs."""
    delta = close.diff()
    up = delta.clip(lower=0.0)
    down = -delta.clip(upper=0.0)
    # Wilder's smoothing
    roll_up = up.ewm(alpha=1 / window, adjust=False).mean()
    roll_down = down.ewm(alpha=1 / window, adjust=False).mean()
    rs = roll_up / (roll_down.replace(0.0, np.nan))
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50.0)


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, window: int = 14) -> pd.Series:
    """Average True Range (ATR)."""
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1 / window, adjust=False).mean()
    return atr


def _zscore(series: pd.Series, window: int = 20) -> pd.Series:
    """Rolling z-score."""
    mean = series.rolling(window, min_periods=window // 2).mean()
    std = series.rolling(window, min_periods=window // 2).std(ddof=0)
    z = (series - mean) / std.replace(0.0, np.nan)
    return z


def _realized_vol_from_logrets(
    logret: pd.Series, horizon_hours: int, bars_per_hour: Optional[int]
) -> float:
    """
    Estimate per-horizon (h hours) realized volatility (in $ terms per $1 of price)
    using recent log-return standard deviation and square-root-of-time.
    """
    if logret.dropna().empty:
        return 0.0

    # If we know bars/hour (e.g., 1 for hourly, 12 for 5-min), scale properly.
    # Fallback: infer bars/hour from average spacing.
    if bars_per_hour is None:
        bars_per_hour = 1

    # Recent window for vol estimate (about one trading day of bars)
    win = max(10, bars_per_hour * 6)
    sigma_bar = float(logret.tail(win).std(ddof=0))  # per-bar log-ret std
    sigma_h = sigma_bar * math.sqrt(horizon_hours * bars_per_hour)
    return sigma_h


# -----------------------------------
# Public API expected by signals.py
# -----------------------------------

def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add core features needed downstream.
    Expected columns: 'Close' (required), 'High'/'Low' (optional but recommended).
    Returns a NEW DataFrame (does not mutate input).
    """
    if df is None or df.empty or "Close" not in df.columns:
        raise ValueError("add_features: input DataFrame must contain 'Close' and not be empty.")

    out = df.copy()

    # Basic returns
    out["ret"] = out["Close"].pct_change()
    out["logret"] = np.log(out["Close"]).diff()

    # Momentum/mean-reversion proxies
    out["rsi14"] = _rsi(out["Close"], 14)
    out["z20"] = _zscore(out["Close"], 20)

    # Volatility proxies
    if all(c in out.columns for c in ["High", "Low", "Close"]):
        out["atr14"] = _atr(out["High"], out["Low"], out["Close"], 14)
    else:
        # Graceful fallback if High/Low missing
        out["atr14"] = (out["Close"].rolling(14).max() - out["Close"].rolling(14).min()).ffill()

    # Rolling realized vol on log returns (per bar)
    out["rv_bar_20"] = out["logret"].rolling(20, min_periods=10).std(ddof=0)

    # Moving averages for trend context (not directly used in forecast but useful for scoring)
    out["ema20"] = out["Close"].ewm(span=20, adjust=False).mean()
    out["ema50"] = out["Close"].ewm(span=50, adjust=False).mean()

    return out


def latest_snapshot(
    df_with_features: pd.DataFrame,
    horizon_hours: int = 2,
    iv_revert_pts: float = 0.5,
    bars_per_hour: Optional[int] = None,
) -> Optional[Dict]:
    """
    Produce a single snapshot dict for the most recent bar including:
      - exp_dS       (expected $ move over horizon)
      - exp_dIV_pts  (expected IV change in points)
      - aux context: rsi14, z20, atr14, rv_bar_20

    Heuristic:
      exp_dS = -meanRevertWeight * z20 * (price * sigma_h)
      where sigma_h is square-root-of-time scaled recent realized vol (from log returns).
      meanRevertWeight increases when RSI is extended (<35 or >65).
    """
    if df_with_features is None or df_with_features.empty:
        return None

    last = df_with_features.iloc[-1]
    price = float(last["Close"])

    # Recent realized vol (log space), then convert to $ move scale by multiplying with price.
    sigma_h_log = _realized_vol_from_logrets(
        df_with_features["logret"], horizon_hours, bars_per_hour
    )
    sigma_h_dollars = price * sigma_h_log  # approximate expected $ stdev over horizon

    # Mean-reversion weight based on RSI stretch
    rsi = float(last.get("rsi14", 50.0))
    if rsi >= 65:
        mr_w = 1.0 + (rsi - 65) / 35.0  # up to ~2.0 if RSI→100
        direction = -1.0  # expect some giveback
    elif rsi <= 35:
        mr_w = 1.0 + (35 - rsi) / 35.0
        direction = +1.0  # expect bounce
    else:
        mr_w = 0.6  # mild reversion bias when neutral
        direction = 0.0 if abs(float(last.get("z20", 0.0))) < 0.3 else -np.sign(float(last.get("z20", 0.0)))

    z = float(last.get("z20", 0.0))
    # Combine zscore & RSI bias
    reversion_signal = direction if direction != 0.0 else -np.sign(z)
    magnitude = min(2.5, abs(z))  # cap effect

    exp_dS = mr_w * magnitude * reversion_signal * sigma_h_dollars

    # Simple IV mean-reversion heuristic: small pull toward recent median
    exp_dIV_pts = float(np.clip(iv_revert_pts, -2.0, 2.0))

    snapshot = {
        "price": price,
        "rsi14": rsi,
        "z20": z,
        "atr14": float(last.get("atr14", np.nan)),
        "rv_bar_20": float(last.get("rv_bar_20", np.nan)),
        "sigma_h_log": float(sigma_h_log),
        "sigma_h_$": float(sigma_h_dollars),
        "exp_dS": float(exp_dS),
        "exp_dIV_pts": float(exp_dIV_pts),
        "horizon_h": int(horizon_hours),
    }
    return snapshot


def latest_snapshots(
    df_with_features_map: Dict[str, pd.DataFrame],
    horizon_hours: int = 2,
    iv_revert_pts: float = 0.5,
    bars_per_hour: Optional[int] = None,
) -> Dict[str, Dict]:
    """
    Convenience wrapper: run latest_snapshot for multiple tickers.
    Input: {symbol: df_with_features}
    Output: {symbol: snapshot_dict}
    """
    out: Dict[str, Dict] = {}
    for sym, dfx in (df_with_features_map or {}).items():
        try:
            snap = latest_snapshot(
                dfx, horizon_hours=horizon_hours, iv_revert_pts=iv_revert_pts, bars_per_hour=bars_per_hour
            )
            if snap is not None:
                out[sym] = snap
        except Exception as e:
            # Keep going even if one symbol fails
            out[sym] = {"error": str(e)}
    return out