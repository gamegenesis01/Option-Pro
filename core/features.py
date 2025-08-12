# core/features.py
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Tuple

import numpy as np
import pandas as pd
import yfinance as yf


# -----------------------------
# Config defaults (can be tuned)
# -----------------------------
HISTORY_DAYS = 45             # lookback for intraday returns (trading days)
MIN_BARS = 60                 # minimum 1h bars to consider forecast “ok”
IV_BOUNDS = (1.0, 200.0)      # % implied vol sanity bounds
DELTA_BOUNDS = (0.05, 0.95)   # keep away from 0/1 deltas
MIN_OI = 50                   # min open interest
MAX_SPREAD_PCT = 35.0         # % of mid
MONEYNESS_PCT = 8.0           # +/-% around spot for near-money filter


@dataclass
class Forecast:
    spot: float
    exp_dS: float        # expected absolute move in underlying over horizon (in $)
    dvol_pts: float      # expected IV change in vol points (e.g., 0.25, 0.5, 1.0)
    horizon_h: int
    source: str          # 'mad', 'realized', or 'fallback'


def _mad_scale(x: pd.Series) -> float:
    """Robust scale estimator: median absolute deviation (MAD) scaled to sigma."""
    med = np.nanmedian(x)
    mad = np.nanmedian(np.abs(x - med))
    # For normal data, sigma ≈ MAD / 0.6745
    return mad / 0.6745 if mad > 0 else np.nan


def get_hourly_history(ticker: str, days: int = HISTORY_DAYS) -> pd.DataFrame:
    df = yf.download(ticker, period=f"{days}d", interval="1h", auto_adjust=True, progress=False)
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()
    # Ensure clean column names and drop duplicates/NAs
    df = df.copy()
    df = df[~df.index.duplicated(keep="last")]
    for col in ("Open", "High", "Low", "Close"):
        if col not in df.columns:
            return pd.DataFrame()
    df = df.dropna(subset=["Close"])
    return df


def forecast_move(df: pd.DataFrame, horizon_h: int) -> Tuple[float, str]:
    """
    Return expected absolute *fractional* move over horizon (|ΔS|/S),
    using robust intraday stats with fallback to realized volatility.
    """
    # 1h log returns
    ret = np.log(df["Close"]).diff().dropna()
    if ret.size < MIN_BARS:
        # fallback to simple close-to-close realized over longer window
        realized = float(ret.std()) if ret.size > 5 else np.nan
        if not np.isfinite(realized) or realized <= 0:
            return 0.0, "fallback"
        sigma_1h = realized
        return abs(sigma_1h) * math.sqrt(max(horizon_h, 1)), "realized"

    # Robust: MAD-based sigma for 1h returns
    sigma_robust = _mad_scale(ret)
    if np.isfinite(sigma_robust) and sigma_robust > 0:
        sigma_h = sigma_robust * math.sqrt(max(horizon_h, 1))
        return float(abs(sigma_h)), "mad"

    # Fallback to standard deviation if MAD fails
    std = float(ret.std())
    if not np.isfinite(std) or std <= 0:
        return 0.0, "fallback"
    return abs(std) * math.sqrt(max(horizon_h, 1)), "realized"


def estimate_dvol_points(df: pd.DataFrame, horizon_h: int) -> float:
    """
    Crude but data-driven ΔIV regime: classify the *current* intraday volatility regime
    and map it to a likely IV shift over the next few hours.

    - Compute rolling 1h absolute returns; compare last value to its 60-day percentiles.
    - Map to vol points:
        small  (<=50th pct) -> +0.25
        medium (50-80th)    -> +0.50
        large  (>80th)      -> +1.00
    """
    r1 = np.log(df["Close"]).diff().abs().dropna()
    if r1.size < 50:
        return 0.5  # conservative default

    last = float(r1.iloc[-1])
    p50, p80 = np.percentile(r1.values, [50, 80])

    if last <= p50:
        return 0.25
    if last <= p80:
        return 0.50
    return 1.00


def build_forecast(ticker: str, horizon_h: int) -> Forecast | None:
    df = get_hourly_history(ticker)
    if df.empty:
        return None

    spot = float(df["Close"].iloc[-1])
    frac_move, src = forecast_move(df, horizon_h)
    dS = abs(frac_move) * spot  # dollar move
    dvol = estimate_dvol_points(df, horizon_h)
    return Forecast(spot=spot, exp_dS=dS, dvol_pts=dvol, horizon_h=horizon_h, source=src)


def within_moneyness(spot: float, strike: float, pct: float = MONEYNESS_PCT) -> bool:
    lo = spot * (1 - pct / 100.0)
    hi = spot * (1 + pct / 100.0)
    return (strike >= lo) and (strike <= hi)


def sanitize_chain(chain_df: pd.DataFrame, spot: float) -> pd.DataFrame:
    """
    Clean & filter the raw options chain:
    - Fix bid>ask inversions by swapping
    - Drop rows with invalid IV, delta, or prices
    - Enforce OI and spread% constraints
    - Enforce near-money window
    """
    df = chain_df.copy()

    # Normalize column names (in case provider uses lowercase)
    cols = {c.lower(): c for c in df.columns}
    # Expected columns: ['symbol','expiry','type','strike','bid','ask','mid','iv','delta','gamma','theta_day','vega','rho','open_interest']
    req = ["symbol", "expiry", "type", "strike", "bid", "ask", "mid", "iv", "delta", "gamma", "theta_day", "vega", "rho", "open_interest"]
    for c in req:
        if c not in df.columns:
            # Try lowercase
            lc = c.lower()
            if lc in cols:
                df.rename(columns={cols[lc]: c}, inplace=True)
            else:
                # If still missing, create safe default
                if c in ("rho",):
                    df[c] = 0.0
                else:
                    df[c] = np.nan

    # Fix bid/ask inversions
    mask_inv = df["bid"] > df["ask"]
    if mask_inv.any():
        b = df.loc[mask_inv, "bid"].copy()
        a = df.loc[mask_inv, "ask"].copy()
        df.loc[mask_inv, "bid"] = a
        df.loc[mask_inv, "ask"] = b

    # Recompute mid when needed
    df["mid"] = np.where(df["mid"].isna() | (df["mid"] <= 0), (df["bid"] + df["ask"]) / 2.0, df["mid"])

    # Drop non-positive or NaN mids
    df = df[np.isfinite(df["mid"]) & (df["mid"] > 0)]

    # Spread %
    df["spread_pct"] = np.where(df["mid"] > 0, (df["ask"] - df["bid"]) / df["mid"] * 100.0, np.inf)

    # Filters
    iv_lo, iv_hi = IV_BOUNDS
    df = df[
        (np.isfinite(df["iv"])) & (df["iv"] >= iv_lo) & (df["iv"] <= iv_hi) &
        (np.isfinite(df["delta"])) & (df["delta"].abs() >= DELTA_BOUNDS[0]) & (df["delta"].abs() <= DELTA_BOUNDS[1]) &
        (np.isfinite(df["open_interest"])) & (df["open_interest"] >= MIN_OI) &
        (np.isfinite(df["spread_pct"])) & (df["spread_pct"] <= MAX_SPREAD_PCT) &
        df["strike"].apply(lambda k: within_moneyness(spot, float(k), MONEYNESS_PCT))
    ]

    # Keep reasonable gammas/vegas if present
    for col in ("gamma", "vega", "theta_day"):
        if col in df.columns:
            df = df[np.isfinite(df[col])]

    return df.reset_index(drop=True)


def taylor_expected_change(row: pd.Series, exp_dS: float, dvol_pts: float, horizon_h: int) -> Tuple[float, float]:
    """
    Taylor approximation of option price change over horizon.
    - exp_change: dollars
    - exp_roi: percent of mid
    """
    delta = float(row.get("delta", 0.0))
    gamma = float(row.get("gamma", 0.0))
    theta_day = float(row.get("theta_day", 0.0))  # $/day
    vega = float(row.get("vega", 0.0))
    rho = float(row.get("rho", 0.0))
    mid = float(row.get("mid", np.nan))

    if not np.isfinite(mid) or mid <= 0:
        return (np.nan, np.nan)

    dS = exp_dS
    dt_days = max(horizon_h, 1) / 24.0
    dIV = dvol_pts  # already in vol points

    # We assume Δr ≈ 0 over a few hours
    exp_change = (delta * dS) + (0.5 * gamma * (dS ** 2)) + (theta_day * dt_days) + (vega * dIV)
    exp_roi = (exp_change / mid) * 100.0
    return float(exp_change), float(exp_roi)


def score_contracts(chain_df: pd.DataFrame, forecast: Forecast) -> pd.DataFrame:
    """
    Add expected change & ROI to chain, return enriched table sorted by ROI desc.
    """
    if chain_df.empty or forecast is None:
        return pd.DataFrame()

    df = sanitize_chain(chain_df, forecast.spot)
    if df.empty:
        return df

    exp_changes, exp_rois = [], []
    for _, row in df.iterrows():
        chg, roi = taylor_expected_change(row, forecast.exp_dS, forecast.dvol_pts, forecast.horizon_h)
        exp_changes.append(chg)
        exp_rois.append(roi)

    df["exp_change"] = exp_changes
    df["exp_roi"] = exp_rois

    # Drop rows where we couldn't compute
    df = df[np.isfinite(df["exp_roi"])]
    return df.sort_values("exp_roi", ascending=False).reset_index(drop=True)