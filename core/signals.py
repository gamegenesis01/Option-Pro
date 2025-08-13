# core/signals.py
from __future__ import annotations

import traceback
from typing import Dict, List, Tuple

from .fetch_data import get_price_history
from .features import latest_snapshot, add_features

# Simple thresholds you can tune
Z20_MAX_ABS = 2.5  # reject extreme z-scores (likely stale/spike)
MIN_BARS = 20      # need at least 20 daily bars for features


def _ok_history(px) -> bool:
    try:
        return (px is not None) and (not px.empty) and ("close" in px.columns or getattr(px, "name", "") == "close")
    except Exception:
        return False


def _validate_snapshot(symbol: str, px) -> Tuple[bool, str]:
    if not _ok_history(px):
        return False, "bad_snapshot (empty)"

    snap = latest_snapshot(px)
    if not snap["ok"]:
        return False, snap.get("reason", "bad_snapshot")

    # last_close must be finite & positive
    last = snap["last_close"]
    if not (isinstance(last, (int, float)) and last > 0):
        return False, "bad_snapshot (invalid last close)"
    return True, ""


def generate_ranked_ideas(config: Dict) -> Dict:
    """
    Core pipeline: fetch -> validate -> features -> simple ranking.
    Returns dict with 'tier1', 'tier2', 'watch' and 'logs'.
    """
    tickers: List[str] = config.get("tickers", [])
    lookback: int = int(config.get("lookback_days", 60))
    interval: str = config.get("px_interval", "1d")

    tier1, tier2, watch = [], [], []
    logs: List[str] = []

    for symbol in tickers:
        try:
            px = get_price_history(symbol, lookback_days=lookback, interval=interval)

            ok, reason = _validate_snapshot(symbol, px)
            if not ok:
                logs.append(f"- [{symbol}] ⚠ Forecast issue: {reason}")
                continue

            # Enough bars?
            if len(px) < MIN_BARS:
                logs.append(f"- [{symbol}] ⚠ Forecast issue: not_enough_history ({len(px)} bars)")
                continue

            feats = add_features(px)
            if not feats["ok"]:
                logs.append(f"- [{symbol}] ⚠ Forecast issue: {feats.get('reason', 'feature_error')}")
                continue

            # Guard z-score sanity
            z20 = feats.get("z20", 0.0)
            if z20 is not None and abs(z20) > Z20_MAX_ABS:
                logs.append(f"- [{symbol}] ⚠ Forecast issue: extreme_zscore ({z20:.2f})")
                continue

            # Simple rank score (tweak as you like)
            score = 0.0
            ma5 = feats.get("ma5")
            ma20 = feats.get("ma20")
            if ma5 and ma20:
                score += (ma5 - ma20) / ma20

            score += feats.get("ret_1d", 0.0) * 0.25

            idea = {"symbol": symbol, "score": score, "last": feats["last_close"]}

            # Tiers (example):
            if score >= 0.02:
                tier1.append(idea)
            elif score >= 0.0:
                tier2.append(idea)
            else:
                watch.append(idea)

        except Exception as e:
            logs.append(f"- [{symbol}] ⚠ Unexpected: {e}")
            tb = traceback.format_exc(limit=1)
            logs.append(f"    {tb.splitlines()[-1]}")

    # Sort by score desc
    tier1.sort(key=lambda x: x["score"], reverse=True)
    tier2.sort(key=lambda x: x["score"], reverse=True)
    watch.sort(key=lambda x: x["score"], reverse=True)

    return {"tier1": tier1, "tier2": tier2, "watch": watch, "logs": logs}