# core/signals.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Any

import math
import numpy as np
import pandas as pd

# Project imports
from core.fetch_data import get_price_history             # price history (1h bars)
from core.features import add_features, latest_snapshot   # builds exp_dS, exp_dIV
from core.options import get_option_chain_near_money      # pulls near-the-money OC
from core.filter_options import filter_contracts          # <- you'll implement/edit
from core.scoring import score_contracts                  # ranks contracts


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
    theta_day: float
    vega: float
    rho: float
    exp_dS: float
    exp_dIV_pts: float
    horizon_h: int
    exp_change: float
    exp_roi: float


def _expect_pnl_from_greeks(
    *,
    mid: float,
    delta: float,
    gamma: float,
    theta_day: float,
    vega: float,
    rho: float,
    dS: float,
    dIV_pts: float,
    horizon_h: int,
    dr: float = 0.0,
) -> Tuple[float, float]:
    """
    Taylor expansion around current point.
    Returns (exp_change_in_premium, exp_roi_percent).
    """
    if mid <= 0 or any(map(lambda x: np.isnan(x) or np.isinf(x),
                           [mid, delta, gamma, theta_day, vega, rho, dS, dIV_pts])):
        return 0.0, 0.0

    dIV = float(dIV_pts) / 100.0
    dt_days = horizon_h / 24.0

    exp_change = (
        delta * dS +
        0.5 * gamma * (dS ** 2) +
        theta_day * dt_days +
        vega * dIV +
        rho * dr * dt_days
    )

    exp_roi = 100.0 * (exp_change / mid) if mid > 0 else 0.0
    return float(exp_change), float(exp_roi)


def _default_filter_cfg() -> Dict[str, Any]:
    """
    Reasonable defaults; override in main() via kwargs.
    You can tighten/loosen these in your own filter_options.py implementation.
    """
    return {
        "max_spread_pct": 35.0,
        "min_open_interest": 100,
        "max_abs_delta": 0.92,       # avoid deep ITM
        "min_price": 0.15,           # avoid sub-dime junk
        "max_price": 25.0,           # avoid super expensive
        "dte_min": 0,
        "dte_max": 14,
    }


def _short_issue(tag: str) -> str:
    return tag


def generate_ranked_ideas(
    symbols: List[str],
    *,
    horizon_hours: int = 2,
    exp_iv_bps: int = 50,            # +50 bps = +0.5 IV point default
    min_roi_pct: float = 18.0,       # floor for Tiering
    filter_cfg: Dict[str, Any] | None = None,
    max_per_tier: int = 10,
    debug: bool = True,
) -> Dict[str, Any]:
    """
    Main entry: builds ranked ideas across a symbol list.
    Returns dict with keys: tier1, tier2, watch, all, logs
    """
    filter_cfg = dict(_default_filter_cfg(), **(filter_cfg or {}))
    logs: List[str] = []
    all_candidates: List[Idea] = []

    for sym in symbols:
        try:
            # 1) price history + features
            px = get_price_history(sym, days=30, interval="1h")
            if px is None or len(px) < 40 or "Close" not in px:
                logs.append(f"[{sym}] ⚠ Forecast issue: {_short_issue('bad_returns')}")
                continue

            px = add_features(px)
            snap = latest_snapshot(px, horizon_hours=horizon_hours)
            if snap is None or "exp_dS" not in snap or "exp_dIV_pts" not in snap:
                logs.append(f"[{sym}] ⚠ Forecast issue: {_short_issue('no_snapshot')}")
                continue

            exp_dS = float(snap["exp_dS"])
            exp_dIV_pts = float(snap["exp_dIV_pts"])

            # 2) option chain
            chain_df = get_option_chain_near_money(sym)
            if chain_df is None or chain_df.empty:
                logs.append(f"[{sym}] ⚠ No option chain")
                continue

            # 3) apply liquidity/quality filters (you’ll maintain the internals)
            filtered = filter_contracts(chain_df, filter_cfg)

            if filtered is None or len(filtered) == 0:
                logs.append(f"[{sym}] ⚠ No liquid near-money contracts after filters.")
                continue

            # 4) compute expected PnL + ROI from Greeks
            ideas_sym: List[Idea] = []
            for row in filtered:
                try:
                    mid = float(row.get("mid", np.nan))
                    bid = float(row.get("bid", np.nan))
                    ask = float(row.get("ask", np.nan))
                    iv = float(row.get("iv", np.nan))

                    delta = float(row.get("delta", np.nan))
                    gamma = float(row.get("gamma", np.nan))
                    theta_day = float(row.get("theta_day", np.nan))
                    vega = float(row.get("vega", np.nan))
                    rho = float(row.get("rho", 0.0))

                    exp_change, exp_roi = _expect_pnl_from_greeks(
                        mid=mid, delta=delta, gamma=gamma,
                        theta_day=theta_day, vega=vega, rho=rho,
                        dS=exp_dS, dIV_pts=exp_iv_bps / 100.0 * 100,  # preserve pts
                        horizon_h=horizon_hours, dr=0.0,
                    )

                    idea = Idea(
                        symbol=sym,
                        expiry=str(row.get("expiry")),
                        type=str(row.get("type")),
                        strike=float(row.get("strike")),
                        mid=mid,
                        bid=bid,
                        ask=ask,
                        iv=iv,
                        delta=delta,
                        gamma=gamma,
                        theta_day=theta_day,
                        vega=vega,
                        rho=rho,
                        exp_dS=exp_dS,
                        exp_dIV_pts=float(exp_iv_bps) / 100.0,  # pts
                        horizon_h=horizon_hours,
                        exp_change=exp_change,
                        exp_roi=exp_roi,
                    )
                    ideas_sym.append(idea)
                except Exception:
                    # Skip bad rows quietly; keep going
                    continue

            if not ideas_sym:
                logs.append(f"[{sym}] ⚠ No candidates post-PnL calc.")
                continue

            # 5) score & order within symbol (uses your scoring.py)
            ideas_sym_sorted = score_contracts(ideas_sym)

            # collect to global list
            all_candidates.extend(ideas_sym_sorted)

            logs.append(f"[{sym}] ⚠ Forecast issue: None")
        except Exception as e:
            logs.append(f"[{sym}] ⚠ Unhandled: {e}")

    if not all_candidates:
        return {
            "tier1": [],
            "tier2": [],
            "watch": [],
            "all": [],
            "logs": logs,
        }

    # Global ordering by expected ROI (desc), with your scoring already applied
    all_candidates.sort(key=lambda x: (x.exp_roi, -abs(x.delta)), reverse=True)

    # Tiering
    tier1 = [asdict(i) for i in all_candidates if i.exp_roi >= max(30.0, min_roi_pct)][:max_per_tier]
    tier2 = [asdict(i) for i in all_candidates if 18.0 <= i.exp_roi < max(30.0, min_roi_pct)][:max_per_tier]

    # Watchlist: best remaining (top fallback)
    remaining = [i for i in all_candidates if asdict(i) not in tier1 + tier2]
    watch = [asdict(i) for i in remaining[:max_per_tier]]

    # “all” flattened (cap to keep emails sane)
    all_dump = [asdict(i) for i in all_candidates[:100]]

    if debug:
        print(f"[DEBUG] ideas -> tier1:{len(tier1)} | tier2:{len(tier2)} | watch:{len(watch)} | all:{len(all_dump)}")

    return {
        "tier1": tier1,
        "tier2": tier2,
        "watch": watch,
        "all": all_dump,
        "logs": logs,
    }


def asdict(i: Idea) -> Dict[str, Any]:
    return {
        "symbol": i.symbol,
        "expiry": i.expiry,
        "type": i.type,
        "strike": i.strike,
        "mid": i.mid,
        "bid": i.bid,
        "ask": i.ask,
        "iv": i.iv,
        "delta": i.delta,
        "gamma": i.gamma,
        "theta_day": i.theta_day,
        "vega": i.vega,
        "rho": i.rho,
        "exp_dS": i.exp_dS,
        "exp_dIV_pts": i.exp_dIV_pts,
        "horizon_h": i.horizon_h,
        "exp_change": i.exp_change,
        "exp_roi": i.exp_roi,
    }