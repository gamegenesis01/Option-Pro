# core/options.py
"""
Utilities to fetch and filter option chains in a liquidity-aware way,
and compute Black–Scholes Greeks for ranking.

Exports
-------
get_option_chain_near_money(symbol, expiry=None, moneyness=0.08,
                            min_open_interest=100, max_spread_pct=0.35,
                            include=('call', 'put'), risk_free=0.03)

get_best_near_money(symbol, top_n=6, **kwargs)  # convenience wrapper
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import yfinance as yf


# ---------- Black–Scholes Greeks (no external dependency) ----------
# All rates and vol as decimals (e.g., 0.25 for 25%).
# T is years to expiry (ACT/365).


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return (1.0 / math.sqrt(2.0 * math.pi)) * math.exp(-0.5 * x * x)


def _d1_d2(S: float, K: float, T: float, r: float, sigma: float) -> Tuple[float, float]:
    if S <= 0 or K <= 0 or sigma <= 0 or T <= 0:
        # Degenerate case: avoid divide-by-zero; return safely
        return 0.0, 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return d1, d2


def bs_greeks(S: float, K: float, T: float, r: float, sigma: float, opt_type: str):
    """
    Return (delta, gamma, theta, vega, rho) for a single option.
    Theta is PER DAY (not per year) to match retail platforms.
    """
    opt_type = opt_type.lower()
    d1, d2 = _d1_d2(S, K, T, r, sigma)
    Nd1 = _norm_cdf(d1)
    Nd2 = _norm_cdf(d2)
    nd1 = _norm_pdf(d1)

    disc = math.exp(-r * T)
    vega = S * nd1 * math.sqrt(T) / 100.0  # per 1 vol point
    gamma = nd1 / (S * sigma * math.sqrt(T)) if S > 0 and sigma > 0 and T > 0 else 0.0

    if opt_type == "call":
        delta = Nd1
        theta_year = (-(S * nd1 * sigma) / (2 * math.sqrt(T)) - r * K * disc * Nd2)
        rho = K * T * disc * Nd2 / 100.0
    else:  # put
        delta = Nd1 - 1.0
        theta_year = (-(S * nd1 * sigma) / (2 * math.sqrt(T)) + r * K * disc * _norm_cdf(-d2))
        rho = -K * T * disc * _norm_cdf(-d2) / 100.0

    theta_per_day = theta_year / 365.0
    return float(delta), float(gamma), float(theta_per_day), float(vega), float(rho)


# ---------- Helpers ----------

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _years_to(expiry_str: str) -> float:
    # expiry in 'YYYY-MM-DD' (yfinance format)
    dt = datetime.strptime(expiry_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    days = max((dt - _now_utc()).total_seconds() / 86400.0, 0.0)
    return days / 365.0


@dataclass
class FilterCfg:
    moneyness: float = 0.08           # ±8% around spot
    min_open_interest: int = 100
    max_spread_pct: float = 0.35      # (ask-bid)/mid <= 35%
    include: Tuple[str, ...] = ("call", "put")
    risk_free: float = 0.03           # 3% annualized


def _pick_nearest_expiry(t: yf.Ticker) -> Optional[str]:
    try:
        expiries = t.options
        return expiries[0] if expiries else None
    except Exception:
        return None


def _mid(bid: float, ask: float) -> float:
    if bid is None or ask is None:
        return np.nan
    m = (float(bid) + float(ask)) / 2.0
    return m if m > 0 else np.nan


def _spread_pct(bid: float, ask: float) -> float:
    m = _mid(bid, ask)
    if not np.isfinite(m) or m <= 0:
        return np.inf
    return max(ask - bid, 0.0) / m


# ---------- Main API ----------

def get_option_chain_near_money(
    symbol: str,
    expiry: Optional[str] = None,
    moneyness: float = 0.08,
    min_open_interest: int = 100,
    max_spread_pct: float = 0.35,
    include: Iterable[str] = ("call", "put"),
    risk_free: float = 0.03,
) -> pd.DataFrame:
    """
    Fetch and return a liquidity-filtered, near-the-money option chain with Greeks.

    Returns a DataFrame with columns:
    ['symbol','expiry','type','strike','bid','ask','lastPrice','volume','openInterest',
     'impliedVolatility','mid','spread','spread_pct','delta','gamma','theta','vega','rho']
    """
    include = tuple(x.lower() for x in include)
    cfg = FilterCfg(
        moneyness=float(moneyness),
        min_open_interest=int(min_open_interest),
        max_spread_pct=float(max_spread_pct),
        include=include,
        risk_free=float(risk_free),
    )

    tkr = yf.Ticker(symbol)

    # Spot (use last close; safer cross-session)
    hist = tkr.history(period="1d")
    if hist.empty:
        return pd.DataFrame()
    spot = float(hist["Close"].iloc[-1])

    # Expiry
    if expiry is None:
        expiry = _pick_nearest_expiry(tkr)
    if not expiry:
        return pd.DataFrame()

    try:
        chain = tkr.option_chain(expiry)
    except Exception:
        return pd.DataFrame()

    # Assemble calls/puts
    parts: List[pd.DataFrame] = []
    if "call" in cfg.include and hasattr(chain, "calls"):
        c = chain.calls.copy()
        if not c.empty:
            c["type"] = "call"
            parts.append(c)
    if "put" in cfg.include and hasattr(chain, "puts"):
        p = chain.puts.copy()
        if not p.empty:
            p["type"] = "put"
            parts.append(p)

    if not parts:
        return pd.DataFrame()

    df = pd.concat(parts, ignore_index=True)

    # Basic hygiene
    cols_keep = [
        "contractSymbol", "lastPrice", "bid", "ask", "change", "percentChange",
        "volume", "openInterest", "impliedVolatility", "inTheMoney", "contractSize",
        "currency", "strike", "type",
    ]
    df = df[[c for c in cols_keep if c in df.columns]]

    # Compute pricing helpers
    df["mid"] = df.apply(lambda r: _mid(float(r.get("bid", np.nan)), float(r.get("ask", np.nan))), axis=1)
    df["spread"] = df.apply(lambda r: max(float(r.get("ask", 0.0)) - float(r.get("bid", 0.0)), 0.0), axis=1)
    df["spread_pct"] = df.apply(
        lambda r: _spread_pct(float(r.get("bid", 0.0)), float(r.get("ask", 0.0))), axis=1
    )

    # Liquidity & sanity filters
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["strike", "openInterest", "impliedVolatility", "mid"])
    df = df[
        (df["openInterest"] >= cfg.min_open_interest)
        & (df["mid"] > 0)
        & (df["spread_pct"] <= cfg.max_spread_pct)
    ]

    if df.empty:
        return pd.DataFrame()

    # Near-the-money filter
    lo = spot * (1.0 - cfg.moneyness)
    hi = spot * (1.0 + cfg.moneyness)
    df = df[(df["strike"] >= lo) & (df["strike"] <= hi)]
    if df.empty:
        return pd.DataFrame()

    # Greeks
    T = _years_to(expiry)
    if T <= 0:
        return pd.DataFrame()

    def _row_greeks(r):
        iv = float(r.get("impliedVolatility", np.nan))
        if not np.isfinite(iv) or iv <= 0:
            return (np.nan, np.nan, np.nan, np.nan, np.nan)
        return bs_greeks(
            S=spot,
            K=float(r["strike"]),
            T=T,
            r=cfg.risk_free,
            sigma=float(iv),
            opt_type=str(r["type"]),
        )

    greeks_arr = np.array([_row_greeks(r) for _, r in df.iterrows()])
    df["delta"] = greeks_arr[:, 0]
    df["gamma"] = greeks_arr[:, 1]
    df["theta"] = greeks_arr[:, 2]
    df["vega"] = greeks_arr[:, 3]
    df["rho"] = greeks_arr[:, 4]

    # Final shape + metadata
    df.insert(0, "symbol", symbol)
    df.insert(1, "expiry", expiry)

    # Sort: tight markets + liquid first
    df = df.sort_values(
        by=["spread_pct", "openInterest", "volume"],
        ascending=[True, False, False],
        ignore_index=True,
    )

    # Friendly types
    numeric_cols = ["lastPrice", "bid", "ask", "volume", "openInterest", "impliedVolatility",
                    "strike", "mid", "spread", "spread_pct", "delta", "gamma", "theta", "vega", "rho"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def get_best_near_money(
    symbol: str,
    top_n: int = 6,
    **kwargs,
) -> pd.DataFrame:
    """
    Convenience wrapper that returns the top N near-the-money contracts for `symbol`.
    Any kwargs are passed through to `get_option_chain_near_money`.
    """
    df = get_option_chain_near_money(symbol, **kwargs)
    if df.empty:
        return df
    return df.head(int(top_n)).reset_index(drop=True)