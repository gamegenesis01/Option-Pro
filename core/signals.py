# core/signals.py
"""
Generate ranked option ideas using near-the-money chains and a simple
movement/vol forecast translated via Taylor expansion of Black–Scholes.

Public API
----------
generate_ranked_ideas(
    tickers: list[str],
    horizon_hours: int = 2,
    dvol_points: float = 0.5,     # assumed IV change in VOL POINTS (e.g. 0.5 = +0.5%)
    moneyness: float = 0.08,      # ±8% around spot
    min_open_interest: int = 100,
    max_spread_pct: float = 0.35,
    include=("call", "put"),
    risk_free: float = 0.03,
    min_roi: float = 0.03,        # 3% ROI threshold to keep
    top_watch: int = 10,          # if no Tier hits, send best N to watch
) -> dict
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import yfinance as yf

from .options import get_option_chain_near_money


@dataclass
class Idea:
    symbol: str
    expiry: str
    type: str
    strike: float
    mid: float
    bid: float
    ask: float
    iv: float
    delta: float
    gamma: float
    theta: float     # per day
    vega: float      # $ per +1 vol point
    rho: float
    exp_dS: float    # $ underlying move used
    exp_dIV_pts: float
    horizon_h: int
    exp_change: float
    exp_roi: float   # change / mid

    def as_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "expiry": self.expiry,
            "type": self.type,
            "strike": round(self.strike, 2),
            "mid": round(self.mid, 2),
            "bid": round(self.bid, 2),
            "ask": round(self.ask, 2),
            "iv": round(self.iv * 100.0, 2),  # %
            "delta": round(self.delta, 4),
            "gamma": round(self.gamma, 4),
            "theta_day": round(self.theta, 4),
            "vega": round(self.vega, 4),
            "rho": round(self.rho, 4),
            "exp_dS": round(self.exp_dS, 3),
            "exp_dIV_pts": round(self.exp_dIV_pts, 3),
            "horizon_h": self.horizon_h,
            "exp_change": round(self.exp_change, 2),
            "exp_roi": round(self.exp_roi * 100.0, 2),  # %
        }


# --------- tiny price forecaster (hourly vol from last ~15d) ---------

def _hourly_sigma(ticker: str) -> Tuple[Optional[float], Optional[str]]:
    """
    Return (sigma_1h, issue). sigma_1h is the std of hourly log-returns.
    """
    try:
        df = yf.download(ticker, period="15d", interval="1h", progress=False)
        if df.empty:
            return None, "no_price_data"
        close = df["Close"].astype(float).dropna()
        if len(close) < 30:
            return None, "not_enough_bars"
        logret = np.diff(np.log(close.values))
        if logret.size == 0 or np.isnan(logret).all():
            return None, "bad_returns"
        sigma = float(np.nanstd(logret, ddof=0))
        return sigma, None
    except Exception as e:
        return None, f"download_error:{e}"


def _best_directional_move(sigma_1h: float, horizon_h: int) -> float:
    """
    Use sqrt(time) scaling; test both +/-, keep the one that maximizes the
    Taylor-estimated ROI later (we’ll pick sign per contract).
    Here we just return the absolute move size in UNDERLYING $ terms as MULTIPLIER;
    caller converts to dollars using spot price fraction via log ≈ pct for small moves.
    """
    # For small log returns r, price change ≈ S * r
    # Expectation ~ 0, but we assume a typical magnitude sigma * sqrt(h)
    return sigma_1h * math.sqrt(max(horizon_h, 1))


# --------- Taylor expansion to dollars ---------

def _taylor_change(delta: float, gamma: float, theta_day: float, vega: float,
                   dS: float, hours: int, dvol_pts: float) -> float:
    """
    Approx change in option price in $:
        ΔP ≈ Δ*ΔS + 0.5*Γ*(ΔS^2) + Θ*(Δt) + Vega*(Δσ_pts)
    where Δt is days, Vega is $ per vol point, and dvol_pts is vol points (e.g. +0.5)
    """
    dt_days = hours / 24.0
    return (delta * dS) + (0.5 * gamma * (dS ** 2)) + (theta_day * dt_days) + (vega * dvol_pts)


# --------- Main API ---------

def generate_ranked_ideas(
    tickers: List[str],
    horizon_hours: int = 2,
    dvol_points: float = 0.5,
    moneyness: float = 0.08,
    min_open_interest: int = 100,
    max_spread_pct: float = 0.35,
    include: Tuple[str, ...] = ("call", "put"),
    risk_free: float = 0.03,
    min_roi: float = 0.03,
    top_watch: int = 10,
) -> Dict[str, List[dict]]:
    """
    Returns dict with keys: tier1, tier2, watch, all, logs (reasons per symbol).
    """
    tier1: List[Idea] = []
    tier2: List[Idea] = []
    watch: List[Idea] = []
    all_candidates: List[Idea] = []
    logs: List[str] = []

    for sym in tickers:
        # 1) Hourly sigma
        sigma_1h, issue = _hourly_sigma(sym)
        if issue:
            logs.append(f"[{sym}] ⚠ Forecast issue: {issue}")
        if sigma_1h is None:
            # continue, but we can still try options with a conservative small move
            sigma_1h = 0.003  # ~0.3%/hr fallback

        # 2) Spot for converting log move to $ (close from latest bar)
        try:
            h1 = yf.Ticker(sym).history(period="1d")
            if h1.empty:
                logs.append(f"[{sym}] ⚠ No spot price; skipping.")
                continue
            spot = float(h1["Close"].iloc[-1])
        except Exception as e:
            logs.append(f"[{sym}] ⚠ Spot error: {e}")
            continue

        # 3) Option chain (near money, liquid)
        chain = get_option_chain_near_money(
            symbol=sym,
            expiry=None,
            moneyness=moneyness,
            min_open_interest=min_open_interest,
            max_spread_pct=max_spread_pct,
            include=include,
            risk_free=risk_free,
        )
        if chain.empty:
            logs.append(f"[{sym}] ⚠ No liquid near-money contracts after filters.")
            continue

        # 4) Movement magnitude
        # Use typical magnitude in $ terms: dS_abs ≈ S * sigma * sqrt(h)
        dS_abs = spot * _best_directional_move(sigma_1h, horizon_hours)
        if not np.isfinite(dS_abs) or dS_abs <= 0:
            logs.append(f"[{sym}] ⚠ Non-positive dS magnitude; skipping.")
            continue

        # 5) Evaluate both directions per contract (pick best)
        for _, r in chain.iterrows():
            mid = float(r["mid"])
            if not np.isfinite(mid) or mid <= 0:
                continue

            # Greeks
            delta = float(r.get("delta", np.nan))
            gamma = float(r.get("gamma", np.nan))
            theta_day = float(r.get("theta", np.nan))
            vega = float(r.get("vega", np.nan))
            if any(np.isnan([delta, gamma, theta_day, vega])):
                continue

            # Up / down scenarios
            change_up = _taylor_change(delta, gamma, theta_day, vega, +dS_abs, horizon_hours, dvol_points)
            change_dn = _taylor_change(delta, gamma, theta_day, vega, -dS_abs, horizon_hours, dvol_points)

            # Pick best
            if change_up >= change_dn:
                best_change, used_dS = change_up, +dS_abs
            else:
                best_change, used_dS = change_dn, -dS_abs

            roi = best_change / mid

            idea = Idea(
                symbol=sym,
                expiry=str(r["expiry"]),
                type=str(r["type"]),
                strike=float(r["strike"]),
                mid=mid,
                bid=float(r.get("bid", np.nan)),
                ask=float(r.get("ask", np.nan)),
                iv=float(r.get("impliedVolatility", np.nan)),
                delta=delta,
                gamma=gamma,
                theta=theta_day,
                vega=vega,
                rho=float(r.get("rho", np.nan)),
                exp_dS=used_dS,
                exp_dIV_pts=float(dvol_points),
                horizon_h=int(horizon_hours),
                exp_change=best_change,
                exp_roi=roi,
            )

            all_candidates.append(idea)

    # Rank all
    all_candidates.sort(key=lambda x: (x.exp_roi, -x.mid), reverse=True)

    # Assign tiers
    for idea in all_candidates:
        if idea.exp_roi >= 0.20:           # >= +20% expected ROI
            tier1.append(idea)
        elif idea.exp_roi >= 0.10:         # 10–20%
            tier2.append(idea)

    # Fallback watchlist if nothing clears threshold
    if not tier1 and not tier2:
        watch = [i for i in all_candidates if i.exp_roi >= min_roi][:top_watch]

    # Build return payload (dict of serializable objects)
    return {
        "tier1": [i.as_dict() for i in tier1[:10]],
        "tier2": [i.as_dict() for i in tier2[:15]],
        "watch": [i.as_dict() for i in watch],
        "all": [i.as_dict() for i in all_candidates[:50]],
        "logs": logs,
    }