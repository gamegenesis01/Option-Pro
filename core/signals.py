# core/signals.py

from __future__ import annotations
from datetime import datetime
from typing import List, Dict, Any

from .features import latest_snapshot
from .options import get_option_chain_near_money
from .filter_options import filter_contracts
from .scoring import score_contracts


def _fmt_issue(e: Exception) -> str:
    return f"{type(e).__name__}: {e}"


def generate_ranked_ideas(
    tickers: List[str],
    *,
    horizon_hours: float | None = None,   # old param (kept for backwards compatibility)
    horizon_min: int | None = None,       # new param
    max_dte_days: int = 14,
    strikes_around: int = 6,
    filter_cfg: Dict[str, Any] | None = None,
    score_cfg: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """
    Build ranked option ideas for a list of tickers.
    Supports both `horizon_hours` and `horizon_min` for backwards compatibility.
    """

    # Backwards compatibility handling
    if horizon_min is None and horizon_hours is not None:
        horizon_min = int(horizon_hours * 60)
    elif horizon_min is None:
        horizon_min = 120  # default 2h

    if filter_cfg is None:
        filter_cfg = {}
    if score_cfg is None:
        score_cfg = {}

    logs: List[str] = []
    aggregate_candidates: List[Dict[str, Any]] = []

    for sym in tickers:
        try:
            snap = latest_snapshot(sym, horizon_min=horizon_min)
            issue = snap.get("issue")
            logs.append(f"[{sym}] ⚠ Forecast issue: {issue}" if issue else f"[{sym}] ✅ Forecast OK")
        except Exception as e:
            logs.append(f"[{sym}] ⚠ Forecast error: {_fmt_issue(e)}")
            continue

        try:
            raw_chain = get_option_chain_near_money(
                sym,
                max_dte=max_dte_days,
                strikes_around=strikes_around,
            )
        except Exception as e:
            logs.append(f"[{sym}] ⚠ Chain fetch error: {_fmt_issue(e)}")
            continue

        for row in raw_chain:
            row["exp_dS"] = snap.get("exp_dS", 0.0)
            row["exp_dIV_pts"] = snap.get("exp_dIV_pts", 0.0)
            row["horizon_h"] = round(horizon_min / 60.0, 4)

        try:
            filtered = filter_contracts(raw_chain, **filter_cfg)
            if not filtered:
                logs.append(f"[{sym}] ⚠ No contracts passed filters")
                continue
        except Exception as e:
            logs.append(f"[{sym}] ⚠ Filter error: {_fmt_issue(e)}")
            continue

        aggregate_candidates.extend(filtered)

    try:
        ranked = score_contracts(aggregate_candidates, **score_cfg)
    except Exception as e:
        logs.append(f"[GLOBAL] ❌ Scoring error: {_fmt_issue(e)}")
        ranked = {"tier1": [], "tier2": [], "watch": [], "all": []}

    return {
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