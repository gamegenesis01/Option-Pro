# core/signals.py
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

# Local deps
from core.fetch_data import get_price_history
from core.options import get_option_chain_near_money
from core.filter_options import filter_contracts
from core.scoring import score_contracts


# ---- constants / reasons ----------------------------------------------------

REASON_BAD_RETURNS = "bad_returns"
REASON_BAD_SNAPSHOT = "bad_snapshot"


# ---- helpers ----------------------------------------------------------------

def _make_issue(symbol: str, reason: str, note: str = "") -> str:
    suffix = f" ({note})" if note else ""
    return f"- [{symbol}] ⚠ Forecast issue: {reason}{suffix}"


def _to_lower_cols(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df.columns = [str(c).strip().lower() for c in df.columns]
    except Exception:
        pass
    return df


def _normalize_price_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make sure a usable 'close' column exists and is numeric.
    Accept common variants like Close, Adj Close, c, price, etc.
    """
    if df is None or len(df) == 0:
        return df

    df = _to_lower_cols(df).copy()

    # Map common variants to 'close'
    candidates = ["close", "adj_close", "adjclose", "c", "price", "last", "settle"]
    existing = [c for c in candidates if c in df.columns]

    if not existing:
        # If we have OHLC named with capitals, try them (after lowercase this is covered)
        # nothing to do here; will be caught later
        return df

    # If 'close' not present, create it from the first viable candidate
    if "close" not in df.columns:
        df["close"] = df[existing[0]]

    # Ensure numeric
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    return df


def _latest_valid_close(df: pd.DataFrame) -> Tuple[float | None, pd.DataFrame]:
    """
    Returns the most recent non-NaN close and the trimmed df
    (rows up to and including that bar). If nothing valid, returns (None, df).
    """
    if df is None or df.empty:
        return None, df

    df = _normalize_price_cols(df)

    if "close" not in df.columns:
        return None, df

    # Drop NaN closes and keep the last valid bar
    valid = df.dropna(subset=["close"])
    if valid.empty:
        return None, df

    last_close = valid["close"].iloc[-1]
    return float(last_close), valid


# ---- public API -------------------------------------------------------------

@dataclass
class RankedResult:
    tier1: List[Dict[str, Any]]
    tier2: List[Dict[str, Any]]
    watch: List[Dict[str, Any]]
    all: List[Dict[str, Any]]
    logs: List[str]


def generate_ranked_ideas(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main orchestrator used by main.py.

    Expected config keys:
      - tickers: list[str]
      - horizon_hours: int
      - px_interval: str (e.g., '5m','15m','1h','1d')
      - filter_cfg: dict (liquidity/quality filters for contracts)
    """
    tickers: List[str] = config.get("tickers", [])
    horizon_hours: int = int(config.get("horizon_hours", 2))
    px_interval: str = str(config.get("px_interval", "1h"))
    filter_cfg: Dict[str, Any] = config.get("filter_cfg", {}) or {}

    logs: List[str] = []
    all_ranked: List[Dict[str, Any]] = []

    for symbol in tickers:
        try:
            # --- price history
            # NOTE: do NOT pass lookback_days unless your fetcher supports it.
            px = get_price_history(symbol, interval=px_interval)

            last_close, px_valid = _latest_valid_close(px)
            if (last_close is None) or (isinstance(last_close, float) and (math.isnan(last_close) or math.isinf(last_close))):
                logs.append(_make_issue(symbol, REASON_BAD_SNAPSHOT, "invalid last close"))
                continue

            # Basic sanity on returns (avoid single-point or all-zeros series)
            # We only need a few points to proceed
            if px_valid is None or len(px_valid) < 3:
                logs.append(_make_issue(symbol, REASON_BAD_RETURNS))
                continue

            # --- option chain (near-money)
            chain = get_option_chain_near_money(symbol)
            if not chain:
                logs.append(f"- [{symbol}] ⚠ No option chain data")
                continue

            # --- filter for quality/liquidity
            filt = filter_contracts(chain, filter_cfg)
            if not filt:
                logs.append(f"- [{symbol}] ⚠ No liquid near-money contracts after filters.")
                continue

            # --- score contracts (your scoring module defines how tiers are decided)
            scored = score_contracts(
                symbol=symbol,
                last_price=last_close,
                contracts=filt,
                horizon_hours=horizon_hours,
                price_series=px_valid,   # give the cleaned series for vol/returns if scorer uses it
            )

            # `score_contracts` should return a list of dicts with at least:
            # { 'symbol','expiry','type','strike','mid','exp_roi','tier', ... }
            if scored:
                all_ranked.extend(scored)

        except Exception as e:
            # Catch-all so one symbol doesn't kill the run
            logs.append(f"- [{symbol}] ⚠ Unexpected: {e.__class__.__name__}: {str(e)}")

    # Partition into tiers
    tier1: List[Dict[str, Any]] = []
    tier2: List[Dict[str, Any]] = []
    watch: List[Dict[str, Any]] = []

    for row in all_ranked:
        tier = str(row.get("tier", "")).lower()
        if tier == "tier1":
            tier1.append(row)
        elif tier == "tier2":
            tier2.append(row)
        elif tier in ("watch", "fallback", "top fallback"):
            watch.append(row)

    # Sort tiers by whatever key your scorer emits (e.g., exp_roi desc)
    def _sort_key(x: Dict[str, Any]):
        return x.get("exp_roi", 0.0)

    tier1.sort(key=_sort_key, reverse=True)
    tier2.sort(key=_sort_key, reverse=True)
    watch.sort(key=_sort_key, reverse=True)
    all_ranked.sort(key=_sort_key, reverse=True)

    return {
        "tier1": tier1,
        "tier2": tier2,
        "watch": watch,
        "all": all_ranked,
        "logs": logs,
    }