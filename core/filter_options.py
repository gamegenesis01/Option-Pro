from __future__ import annotations
from typing import List, Dict, Any, Tuple
import os

def _nz(x, d=0.0):
    try:
        if x is None: return d
        return float(x)
    except Exception:
        return d

def _spread_pct(row: Dict[str, Any]) -> float:
    sp = row.get("spread_pct")
    if sp is not None:
        try: return float(sp)
        except Exception: pass
    b = _nz(row.get("bid"), 0.0)
    a = _nz(row.get("ask"), 0.0)
    m = _nz(row.get("mid"), 0.0)
    if b > 0 and a > 0 and m > 0:
        return max((a - b) / m, 0.0)
    return 1.0

def _tier_split(
    rows: List[Dict[str, Any]],
    min_score_t1: float,
    min_score_t2: float,
    max_items_per_tier: int = 8
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    t1, t2 = [], []
    for r in rows:
        sc = _nz(r.get("score"), 0.0)
        if sc >= min_score_t1 and len(t1) < max_items_per_tier:
            r.setdefault("flags", []).append("Tier1")
            t1.append(r)
        elif sc >= min_score_t2 and len(t2) < max_items_per_tier:
            r.setdefault("flags", []).append("Tier2")
            t2.append(r)
    return t1, t2

def filter_candidates(
    candidates: List[Dict[str, Any]],
    cfg: Dict[str, Any] | None = None
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """
    Looser gating + tiering with a watchlist fallback.
    Returns: (tier1, tier2, watchlist, stats)
    """
    cfg = cfg or {}

    MIN_OI         = int(os.getenv("MIN_OI",        cfg.get("MIN_OI",        50)))
    MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", cfg.get("MAX_SPREAD_PCT", 0.50)))
    DTE_MIN        = int(os.getenv("DTE_MIN",       cfg.get("DTE_MIN",        0)))
    DTE_MAX        = int(os.getenv("DTE_MAX",       cfg.get("DTE_MAX",       21)))
    MIN_PRICE      = float(os.getenv("MIN_PRICE",   cfg.get("MIN_PRICE",     0.10)))
    MAX_PRICE      = float(os.getenv("MAX_PRICE",   cfg.get("MAX_PRICE",    10.00)))
    STRIKES_RANGE  = int(os.getenv("STRIKES_RANGE", cfg.get("STRIKES_RANGE", 8)))

    MIN_SCORE_T1   = float(os.getenv("MIN_SCORE_TIER1", cfg.get("MIN_SCORE_TIER1", 72)))
    MIN_SCORE_T2   = float(os.getenv("MIN_SCORE_TIER2", cfg.get("MIN_SCORE_TIER2", 60)))
    WATCHLIST_TOP  = int(os.getenv("WATCHLIST_TOP", cfg.get("WATCHLIST_TOP", 5)))

    stats = dict(missing_iv=0, thin_oi=0, wide_spread=0, bad_dte=0, bad_price=0,
                 out_of_range=0, ok=0)

    gated: List[Dict[str, Any]] = []
    relaxed: List[Dict[str, Any]] = []

    for r in candidates:
        r = dict(r)
        r.setdefault("flags", [])
        oi   = _nz(r.get("openInterest"), 0.0)
        dte  = float(r.get("dte", r.get("daysToExpiration", 999)))
        mid  = _nz(r.get("mid") or r.get("price") or r.get("lastPrice"), 0.0)
        sc   = _nz(r.get("score"), 0.0)
        spr  = _spread_pct(r)
        rng  = abs(_nz(r.get("strikeDistance", 0.0)))

        if oi < MIN_OI: stats["thin_oi"] += 1
        if spr > MAX_SPREAD_PCT: stats["wide_spread"] += 1
        if dte < DTE_MIN or dte > DTE_MAX: stats["bad_dte"] += 1
        if mid < MIN_PRICE or mid > MAX_PRICE: stats["bad_price"] += 1
        if STRIKES_RANGE and rng > STRIKES_RANGE: stats["out_of_range"] += 1

        hard_ok = (
            oi >= MIN_OI and
            spr <= MAX_SPREAD_PCT and
            (DTE_MIN <= dte <= DTE_MAX) and
            (MIN_PRICE <= mid <= MAX_PRICE) and
            (rng <= STRIKES_RANGE if STRIKES_RANGE else True)
        )

        soft_ok = (
            sc >= MIN_SCORE_T1 and
            oi >= max(20, 0.5 * MIN_OI) and
            spr <= MAX_SPREAD_PCT * 1.25 and
            (DTE_MIN - 1) <= dte <= (DTE_MAX + 5) and
            mid >= max(0.05, MIN_PRICE * 0.75)
        )

        if hard_ok:
            r["flags"].append("ok")
            stats["ok"] += 1
            gated.append(r)
        elif soft_ok:
            r["flags"].append("soft-pass")
            relaxed.append(r)
        else:
            r["flags"].append("reject")

    pool = gated + relaxed
    pool.sort(key=lambda x: _nz(x.get("score"), 0.0), reverse=True)

    tier1, tier2 = _tier_split(pool, MIN_SCORE_T1, MIN_SCORE_T2)

    watchlist: List[Dict[str, Any]] = []
    if len(tier1) + len(tier2) < max(3, WATCHLIST_TOP // 2):
        for r in pool:
            if len(watchlist) >= WATCHLIST_TOP: break
            if r not in tier1 and r not in tier2:
                rr = dict(r)
                rr.setdefault("flags", []).append("watchlist")
                watchlist.append(rr)

    return tier1, tier2, watchlist, stats