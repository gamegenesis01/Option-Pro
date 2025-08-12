# core/signals.py
from __future__ import annotations

import math
import datetime as dt
from typing import Dict, List, Tuple, Any

import numpy as np
import pandas as pd

# Local modules (all already in your repo)
from .fetch_data import get_price_history  # price history (yfinance)
from .forecast import forecast_move        # produces exp_dS and exp_dIV estimate
from .options import get_option_chain_near_money  # pulls a near-money chain
from .filter_options import filter_contracts      # liquidity/quality filters


TRADING_HOURS_PER_DAY = 6.5

DEFAULT_UNIVERSE = [
    "SPY", "AAPL", "TSLA", "MSFT", "AMZN",
    "GOOGL", "NVDA", "META", "NFLX",
    "AMD", "AAL", "PLTR", "F", "RIVN", "SOFI",
]

# -------------------------------
# Helpers
# -------------------------------

def _theta_over_hours(theta_day: float, horizon_hours: float) -> float:
    """Convert daily theta to the horizon contribution (≈ only during RTH)."""
    if TRADING_HOURS_PER_DAY <= 0:
        return 0.0
    frac = horizon_hours / TRADING_HOURS_PER_DAY
    return theta_day * frac


def _expected_change_from_greeks(
    contract: Dict[str, Any],
    exp_dS: float,
    exp_dIV_pts: float,
    horizon_hours: float,
) -> float:
    """
    Taylor expansion on the option P/L given the contract greeks and forecast.
    exp_dIV_pts is in 'percentage points' (e.g., +0.5 means +0.5 vol points).
    """
    delta = float(contract.get("delta", 0.0))
    gamma = float(contract.get("gamma", 0.0))
    theta_day = float(contract.get("theta_day", 0.0))
    vega = float(contract.get("vega", 0.0))
    rho = float(contract.get("rho", 0.0))  # we keep rho * 0 unless you feed a view

    dS = float(exp_dS)
    dIV = float(exp_dIV_pts) / 100.0  # convert pts → decimal
    dt_hours = float(horizon_hours)

    # First and second order price move
    first_order = delta * dS
    second_order = 0.5 * gamma * (dS ** 2)

    # Time decay over the intraday horizon
    theta_contrib = _theta_over_hours(theta_day, dt_hours)

    # Vol contribution
    vega_contrib = vega * dIV

    # No explicit rate view intraday
    rho_contrib = 0.0 * rho

    return first_order + second_order + theta_contrib + vega_contrib + rho_contrib


def _roi_pct(exp_change: float, mid: float) -> float:
    if mid is None or mid <= 0:
        return -np.inf
    return 100.0 * (exp_change / float(mid))


def _tier_from_roi(roi_pct_value: float) -> str | None:
    """
    Tiering logic:
    - Tier 1: ROI ≥ 40%
    - Tier 2: 20% ≤ ROI < 40%
    - None: below 20%
    """
    if roi_pct_value >= 40.0:
        return "tier1"
    if roi_pct_value >= 20.0:
        return "tier2"
    return None


# -------------------------------
# Main entry
# -------------------------------

def generate_ranked_ideas(
    universe: List[str] = None,
    horizon_hours: float = 2.0,
    dte_min: int = 0,
    dte_max: int = 14,
    min_open_interest: int = 100,
    max_spread_pct: float = 0.35,
    price_band_usd: float = 8.0,
    exp_dIV_pts: float = 0.5,   # +0.5 IV pts as a mild default intraday
    use_reversion_bias: bool = True,
) -> Dict[str, Any]:
    """
    Pulls data for each symbol, forecasts an expected underlying move (and IV change),
    evaluates near‑money liquid contracts via Greeks, and ranks them.
    Returns a dict with 'tier1', 'tier2', 'watch', 'all', 'logs'.
    """
    if universe is None:
        universe = DEFAULT_UNIVERSE

    now_ts = dt.datetime.utcnow()

    tier1: List[Dict[str, Any]] = []
    tier2: List[Dict[str, Any]] = []
    watch: List[Dict[str, Any]] = []
    all_list: List[Dict[str, Any]] = []
    logs: List[str] = []

    for symbol in universe:
        # 1) Price history
        try:
            px = get_price_history(symbol, lookback_days=30, interval="1h")
            if px is None or len(px) < 30:
                logs.append(f"- [{symbol}] ⚠ Forecast issue: bad_returns")
                continue
        except Exception as e:
            logs.append(f"- [{symbol}] ⚠ fetch error: {e}")
            continue

        # 2) Forecast expected move (and optional IV view)
        try:
            fc = forecast_move(
                symbol,
                horizon_hours=horizon_hours,
                bias_mode=("revert" if use_reversion_bias else "none"),
            )
            exp_dS = float(fc.get("exp_dS", 0.0))
            # allow override of IV view via arg; if fc provides one, prefer it unless zero
            exp_dIV_pts_eff = exp_dIV_pts
            fc_iv = float(fc.get("exp_dIV_pts", 0.0))
            if abs(fc_iv) > 1e-9:
                exp_dIV_pts_eff = fc_iv
        except Exception as e:
            logs.append(f"- [{symbol}] ⚠ forecast error: {e}")
            continue

        # 3) Pull near-money options
        try:
            chain = get_option_chain_near_money(
                symbol,
                dte_range=(dte_min, dte_max),
                width=5  # a handful of strikes each side
            )
            if chain is None or len(chain) == 0:
                logs.append(f"- [{symbol}] ⚠ no option chain")
                continue
        except Exception as e:
            logs.append(f"- [{symbol}] ⚠ chain error: {e}")
            continue

        # 4) Liquidity/quality filters
        try:
            filt = filter_contracts(
                chain,
                min_oi=min_open_interest,
                max_spread_pct=max_spread_pct,
                max_mid=price_band_usd
            )
            if filt is None or len(filt) == 0:
                logs.append(f"- [{symbol}] ⚠ No liquid near-money contracts after filters.")
                continue
        except Exception as e:
            logs.append(f"- [{symbol}] ⚠ filter error: {e}")
            continue

        # 5) Expected change & ROI per contract
        ranked_bucket: List[Tuple[float, Dict[str, Any]]] = []
        for c in filt:
            try:
                mid = float(c.get("mid", 0.0))
                if not (mid > 0):
                    continue

                exp_chg = _expected_change_from_greeks(
                    contract=c,
                    exp_dS=exp_dS if c.get("type") == "call" else -exp_dS if c.get("type") == "put" else exp_dS,
                    exp_dIV_pts=exp_dIV_pts_eff,
                    horizon_hours=horizon_hours
                )
                roi = _roi_pct(exp_chg, mid)

                out = dict(c)
                out.update({
                    "symbol": symbol,
                    "exp_dS": (exp_dS if c.get("type") == "call" else -exp_dS if c.get("type") == "put" else exp_dS),
                    "exp_dIV_pts": exp_dIV_pts_eff,
                    "horizon_h": horizon_hours,
                    "exp_change": round(exp_chg, 2),
                    "exp_roi": round(roi, 2),
                })
                ranked_bucket.append((roi, out))
            except Exception as e:
                logs.append(f"- [{symbol}] ⚠ calc error: {e}")
                continue

        if not ranked_bucket:
            # nothing scored, move on
            continue

        ranked_bucket.sort(reverse=True, key=lambda x: x[0])
        for _, item in ranked_bucket:
            all_list.append(item)
            tier = _tier_from_roi(item["exp_roi"])
            if tier == "tier1":
                tier1.append(item)
            elif tier == "tier2":
                tier2.append(item)
            else:
                # best of the rest = watch (limit size to keep email readable)
                if len(watch) < 20:
                    watch.append(item)

        # For visibility: if forecast stage was ok but returns looked odd earlier
        logs.append(f"- [{symbol}] ⚠ Forecast issue: None")

    # Sort final groups by ROI descending for nice emails
    tier1.sort(key=lambda d: d.get("exp_roi", -1e9), reverse=True)
    tier2.sort(key=lambda d: d.get("exp_roi", -1e9), reverse=True)
    watch.sort(key=lambda d: d.get("exp_roi", -1e9), reverse=True)
    all_list.sort(key=lambda d: d.get("exp_roi", -1e9), reverse=True)

    return {
        "timestamp": now_ts,
        "tier1": tier1,
        "tier2": tier2,
        "watch": watch,
        "all": all_list,
        "logs": logs,
        # echo key params so the email header shows them
        "config": {
            "horizon_hours": horizon_hours,
            "dte": [dte_min, dte_max],
            "minOI": min_open_interest,
            "maxSpread": max_spread_pct,
            "priceBand": price_band_usd,
            "exp_dIV_pts": exp_dIV_pts,
            "bias": "revert" if use_reversion_bias else "none",
        },
    }


__all__ = ["generate_ranked_ideas"]