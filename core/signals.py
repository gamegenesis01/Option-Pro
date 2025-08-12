# core/signals.py
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from .features import (
    Forecast,
    build_forecast,
    score_contracts,
)

# NOTE: `core.fetch` is expected to provide:
# - list_expiries(ticker: str) -> List[str] (YYYY-MM-DD)
# - get_options_chain(ticker: str, expiry: str | None) -> pd.DataFrame
#   Required columns in the returned DataFrame:
#   ['symbol','expiry','type','strike','bid','ask','mid','iv',
#    'delta','gamma','theta_day','vega','rho','open_interest']
from . import fetch


# -----------------------------
# Tunable signal parameters
# -----------------------------
DEFAULT_HORIZON_H = 2             # forecast horizon in hours
MAX_EXPIRIES_PER_TICKER = 2       # how many near expiries to scan
MIN_ROI_TIER1 = 40.0              # ROI% threshold for Tier-1 ideas
MIN_ROI_TIER2 = 20.0              # ROI% threshold for Tier-2 ideas
TOP_WATCH = 25                    # keep a broader “watch” list per run
MAX_PER_TICKER = 12               # cap per-ticker rows before merging


@dataclass
class SignalResult:
    tier1: List[Dict]
    tier2: List[Dict]
    watch: List[Dict]
    logs: List[str]


def _select_near_expiries(ticker: str, max_n: int = MAX_EXPIRIES_PER_TICKER) -> List[str]:
    """Pick the nearest (soonest) expiries."""
    try:
        expiries = fetch.list_expiries(ticker)
        if not expiries:
            return []
        # Sort YYYY-MM-DD strings chronologically
        expiries = sorted(expiries)[:max_n]
        return expiries
    except Exception as e:
        return []


def _chain_for_expiries(ticker: str, expiries: List[str]) -> pd.DataFrame:
    frames = []
    for exp in expiries:
        try:
            df = fetch.get_options_chain(ticker, expiry=exp)
            if isinstance(df, pd.DataFrame) and not df.empty:
                frames.append(df)
        except Exception:
            # skip quietly; logging is done at the caller
            continue
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    # Ensure canonical dtypes
    for col in ("strike", "bid", "ask", "mid", "iv", "delta", "gamma", "theta_day", "vega", "rho", "open_interest"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def generate_signals(
    tickers: List[str],
    horizon_h: int = DEFAULT_HORIZON_H,
    min_roi_tier1: float = MIN_ROI_TIER1,
    min_roi_tier2: float = MIN_ROI_TIER2,
) -> SignalResult:
    """
    Main signal pipeline:
      1) Build robust intraday forecast (exp_dS, ΔIV points) per ticker
      2) Pull a couple of near expiries, score contracts by Taylor expansion
      3) Bucket into Tier1/Tier2 by ROI%, plus a broader watch list
    Returns plain dict rows so the email step can format easily.
    """
    tier1_rows: List[Dict] = []
    tier2_rows: List[Dict] = []
    watch_rows: List[Dict] = []
    logs: List[str] = []

    for t in tickers:
        try:
            fc: Forecast | None = build_forecast(t, horizon_h=horizon_h)
            if fc is None or not np.isfinite(fc.exp_dS) or fc.exp_dS <= 0:
                logs.append(f"[{t}] ⚠ Forecast issue: bad_returns")
                continue

            exps = _select_near_expiries(t, MAX_EXPIRIES_PER_TICKER)
            if not exps:
                logs.append(f"[{t}] ⚠ No expiries available.")
                continue

            raw_chain = _chain_for_expiries(t, exps)
            if raw_chain.empty:
                logs.append(f"[{t}] ⚠ Empty chain for {exps}.")
                continue

            scored = score_contracts(raw_chain, fc)
            if scored.empty:
                logs.append(f"[{t}] ⚠ No contracts after filters.")
                continue

            # Keep only a handful per ticker before global merge
            scored = scored.head(MAX_PER_TICKER)

            # Attach forecast context columns for downstream formatting
            scored["horizon_h"] = fc.horizon_h
            scored["exp_dS"] = fc.exp_dS
            scored["exp_dIV_pts"] = fc.dvol_pts
            scored["forecast_source"] = fc.source

            # Bucket by ROI thresholds
            t1 = scored[scored["exp_roi"] >= min_roi_tier1]
            t2 = scored[(scored["exp_roi"] >= min_roi_tier2) & (scored["exp_roi"] < min_roi_tier1)]

            def _to_dict_rows(df: pd.DataFrame) -> List[Dict]:
                cols = [
                    "symbol", "expiry", "type", "strike",
                    "mid", "bid", "ask", "iv",
                    "delta", "gamma", "theta_day", "vega", "rho",
                    "exp_dS", "exp_dIV_pts", "horizon_h",
                    "exp_change", "exp_roi"
                ]
                out = []
                for _, r in df.iterrows():
                    row = {c: (float(r[c]) if c in r and pd.notna(r[c]) and isinstance(r[c], (int, float, np.floating))
                               else (r[c] if c in r else None)) for c in cols}
                    # Clean up small negatives due to numeric noise
                    if isinstance(row.get("exp_roi"), float) and abs(row["exp_roi"]) < 1e-6:
                        row["exp_roi"] = 0.0
                    out.append(row)
                return out

            tier1_rows.extend(_to_dict_rows(t1))
            tier2_rows.extend(_to_dict_rows(t2))
            # For watch list we’ll merge later across tickers; keep a bit more breadth:
            watch_rows.extend(_to_dict_rows(scored))

        except Exception as e:
            logs.append(f"[{t}] ⚠ Pipeline error: {e}")

    # Assemble global watch list by ROI desc, trim length
    if watch_rows:
        watch_df = pd.DataFrame(watch_rows)
        watch_df = watch_df.sort_values("exp_roi", ascending=False).head(TOP_WATCH).reset_index(drop=True)
        watch_rows = watch_df.to_dict(orient="records")

    return SignalResult(
        tier1=tier1_rows,
        tier2=tier2_rows,
        watch=watch_rows,
        logs=logs,
    )