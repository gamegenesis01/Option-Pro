# core/signals.py

from __future__ import annotations

from datetime import datetime
from typing import List, Dict, Any

# Internal deps — these names must exist in your repo
from .features import latest_snapshot           # builds exp_dS (expected underlying move) and exp_dIV_pts
from .options import get_option_chain_near_money
from .filter_options import filter_contracts    # liquidity/quality filters
from .scoring import score_contracts            # computes exp_change / exp_roi and returns ranked contracts


def _fmt_issue(e: Exception) -> str:
    return f"{type(e).__name__}: {e}"


def generate_ranked_ideas(
    tickers: List[str],
    *,
    horizon_min: int = 120,         # 2h default horizon
    max_dte_days: int = 14,         # look out to 2 weeks
    strikes_around: int = 6,        # how many strikes each side of spot
    filter_cfg: Dict[str, Any] | None = None,  # passed straight into filter_contracts
    score_cfg: Dict[str, Any] | None = None,   # passed straight into score_contracts
) -> Dict[str, Any]:
    """
    Build ranked option ideas for a list of tickers.

    Parameters
    ----------
    tickers : list[str]
        Symbols to scan (e.g., ["SPY", "AAPL", "MSFT"]).
    horizon_min : int
        Forecast horizon in minutes (used by features.latest_snapshot).
    max_dte_days : int
        Max days-to-expiry to pull from the option chain.
    strikes_around : int
        Number of strikes on each side of near-the-money to request.
    filter_cfg : dict | None
        Settings for liquidity/quality filters (e.g., {"min_oi":100,"max_spread":0.35}).
    score_cfg : dict | None
        Settings for the scoring model (e.g., ROI thresholds, tier cutoffs).

    Returns
    -------
    dict with:
      - tier1, tier2, watch: lists of dicts (contracts)
      - all: full ranked list (after filters)
      - logs: list of text diagnostics per symbol
      - meta: run metadata
    """
    if filter_cfg is None:
        filter_cfg = {}
    if score_cfg is None:
        score_cfg = {}

    logs: List[str] = []
    aggregate_candidates: List[Dict[str, Any]] = []

    for sym in tickers:
        # 1) Forecast step (expected underlying move + IV shift)
        try:
            snap = latest_snapshot(sym, horizon_min=horizon_min)
            # snap is expected to include: {"exp_dS": float, "exp_dIV_pts": float, "issue": Optional[str], ...}
            issue = snap.get("issue")
            if issue:
                logs.append(f"[{sym}] ⚠ Forecast issue: {issue}")
            else:
                logs.append(f"[{sym}] ⚠ Forecast issue: None")
        except Exception as e:
            logs.append(f"[{sym}] ⚠ Forecast error: {_fmt_issue(e)}")
            # If we can’t forecast, skip this symbol entirely
            continue

        # 2) Get a small, near-the-money option universe
        try:
            raw_chain = get_option_chain_near_money(
                sym,
                max_dte=max_dte_days,
                strikes_around=strikes_around,
            )
        except Exception as e:
            logs.append(f"[{sym}] ⚠ Chain fetch error: {_fmt_issue(e)}")
            continue

        # Attach forecast context to each row right away
        for row in raw_chain:
            row["exp_dS"] = snap.get("exp_dS", 0.0)
            row["exp_dIV_pts"] = snap.get("exp_dIV_pts", 0.0)
            row["horizon_h"] = round(horizon_min / 60.0, 4)

        # 3) Liquidity / quality filters
        try:
            filtered = filter_contracts(raw_chain, **filter_cfg)
            if not filtered:
                logs.append(f"[{sym}] ⚠ No liquid near-money contracts after filters.")
                continue
        except Exception as e:
            logs.append(f"[{sym}] ⚠ Filter error: {_fmt_issue(e)}")
            continue

        # Collect for scoring
        aggregate_candidates.extend(filtered)

    # 4) Score & rank across all symbols
    ranked: Dict[str, Any]
    try:
        ranked = score_contracts(aggregate_candidates, **score_cfg)
        # score_contracts should return a dict with keys: tier1, tier2, watch, all
        # and each list contains enriched contract dicts (exp_change, exp_roi, etc.)
    except Exception as e:
        # If scoring fails, return empty tiers but keep logs
        logs.append(f"[GLOBAL] ❌ Scoring error: {_fmt_issue(e)}")
        ranked = {"tier1": [], "tier2": [], "watch": [], "all": []}

    result = {
        "tier1": ranked.get("tier1", []),
        "tier2": ranked.get("tier2", []),
        "watch": ranked.get("watch", []),
        "all": ranked.get("all", []),
        "logs": logs,
        "meta": {
            "timestamp": datetime.utcnow().isoformat(),
            "horizon_min": horizon_min,
            "max_dte_days": max_dte_days,
            "strikes_around": strikes_around,
            "filter_cfg": filter_cfg,
            "score_cfg": score_cfg,
            "universe": list(tickers),
        },
    }
    return result