# core/signals.py
from __future__ import annotations

import math
import traceback
from typing import Dict, List, Any, Tuple

import numpy as np
import pandas as pd

# ---- Local imports (robust to package/relative layout) ----------------------
try:
    from core.fetch_data import get_price_history
except Exception:  # pragma: no cover
    from .fetch_data import get_price_history  # type: ignore

try:
    from core.options import get_option_chain_near_money
except Exception:  # pragma: no cover
    from .options import get_option_chain_near_money  # type: ignore

try:
    from core.features import add_features, latest_snapshot, exp_dS, exp_dIV
except Exception:  # pragma: no cover
    from .features import add_features, latest_snapshot, exp_dS, exp_dIV  # type: ignore

try:
    from core.filter_options import filter_contracts
except Exception:  # pragma: no cover
    from .filter_options import filter_contracts  # type: ignore

try:
    from core.scoring import score_contracts
except Exception:  # pragma: no cover
    from .scoring import score_contracts  # type: ignore


# ---------------------------------------------------------------------------

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
            return default
        return float(x)
    except Exception:
        return default


def _expected_price_change_greeks(
    delta: float,
    gamma: float,
    theta_day: float,
    vega: float,
    rho: float,
    dS: float,
    dIV_pts: float,
    horizon_hours: float,
    dRate: float = 0.0,
) -> float:
    """
    Taylor approximation of option PnL using Greeks.
    theta_day is per-calendar-day; convert for horizon in hours.
    dIV_pts = change in IV in percentage points (e.g. +0.5 pt -> 0.5)
    """
    theta_part = theta_day * (horizon_hours / 24.0)
    vega_part = vega * (dIV_pts / 100.0)  # convert pts to decimal
    rho_part = rho * dRate
    return (delta * dS) + (0.5 * gamma * (dS ** 2)) + theta_part + vega_part + rho_part


def _tier_split(
    rows: List[Dict[str, Any]],
    tier1_min_score: float,
    tier2_min_score: float,
    min_roi: float,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Split ranked rows into tier1 / tier2 / watch by score and ROI."""
    t1, t2, watch = [], [], []
    for r in rows:
        sc = _safe_float(r.get("score"), 0.0)
        roi = _safe_float(r.get("exp_roi"), 0.0)
        if sc >= tier1_min_score and roi >= min_roi:
            t1.append(r)
        elif sc >= tier2_min_score and roi >= min_roi:
            t2.append(r)
        else:
            watch.append(r)
    return t1, t2, watch


def generate_ranked_ideas(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Scan tickers, build features/forecast, pull near-money options,
    filter & score, then compute expected PnL for a horizon.
    Returns:
        {
          "tier1": [...],
          "tier2": [...],
          "watch": [...],
          "all": [...],
          "logs": [...]
        }
    """
    # -------------------- Read config with sensible defaults -----------------
    tickers: List[str] = list(config.get("tickers", []))
    horizon_hours: float = float(config.get("horizon_hours", 2))
    lookback_days: int = int(config.get("lookback_days", 30))
    px_interval: str = str(config.get("px_interval", "1h"))

    # Option filters
    max_dte: int = int(config.get("max_dte", 14))
    strikes_width: float = float(config.get("strikes_width", 8))  # +/- dollars window
    min_oi: int = int(config.get("min_oi", 100))
    max_spread_pct: float = float(config.get("max_spread_pct", 35))  # e.g., 35 (%)

    # Ranking / ROI gates
    tier1_min_score: float = float(config.get("tier1_min_score", 0.80))
    tier2_min_score: float = float(config.get("tier2_min_score", 0.60))
    min_roi: float = float(config.get("min_roi", 15.0))  # percent
    top_k: int = int(config.get("top_k", 40))  # keep many; tiers will trim naturally

    # Biasing of expected IV move
    dIV_pts_default: float = float(config.get("exp_dIV_pts", 0.5))  # +0.5 vol-pts default
    bias_mode: str = str(config.get("bias_mode", "revert")).lower()
    # -------------------------------------------------------------------------

    logs: List[str] = []
    all_rows: List[Dict[str, Any]] = []

    for symbol in tickers:
        try:
            # ------- Prices & features
            px = get_price_history(symbol, lookback_days=lookback_days, interval=px_interval)
            if px is None or len(px) < 25:
                logs.append(f"[{symbol}] ⚠ Forecast issue: not_enough_data")
                continue

            try:
                feat = add_features(px)
            except Exception:
                # proceed even if extra features fail
                feat = px.copy()

            snap = latest_snapshot(px)
            if snap is None or "price" not in snap:
                logs.append(f"[{symbol}] ⚠ Forecast issue: bad_snapshot")
                continue

            # Expected underlying move (dS) & IV change (dIV in vol points)
            try:
                dS = float(exp_dS(px, horizon_hours=horizon_hours, bias_mode=bias_mode))
            except Exception:
                # Fallback: use recent hourly vol
                rets = np.diff(np.log(px["Close"].astype(float).values))
                dS = float(np.std(rets[-48:]) * snap["price"]) if len(rets) > 0 else 0.0

            try:
                dIV_pts = float(exp_dIV(px))
            except Exception:
                dIV_pts = dIV_pts_default

            # ------- Option chain (near-money within +/- strikes_width, <= max_dte)
            try:
                chain = get_option_chain_near_money(
                    symbol,
                    snapshot=snap,
                    max_dte=max_dte,
                    strikes_width=strikes_width,
                )
            except TypeError:
                # older signature fallback
                try:
                    chain = get_option_chain_near_money(symbol, snap)
                except Exception as e_chain:
                    logs.append(f"[{symbol}] ⚠ Chain error: {e_chain}")
                    continue
            except Exception as e_chain:
                logs.append(f"[{symbol}] ⚠ Chain error: {e_chain}")
                continue

            if not isinstance(chain, list) or len(chain) == 0:
                logs.append(f"[{symbol}] ⚠ No contracts pulled")
                continue

            # ------- Liquidity/quality filters
            try:
                filtered = filter_contracts(
                    chain,
                    min_oi=min_oi,
                    max_spread_pct=max_spread_pct,
                    strikes_width=strikes_width,
                    max_dte=max_dte,
                )
            except Exception:
                # permissive fallback: manual filter
                filtered = []
                for c in chain:
                    bid = _safe_float(c.get("bid"))
                    ask = _safe_float(c.get("ask"))
                    mid = _safe_float(c.get("mid"), (bid + ask) / 2.0 if (bid > 0 and ask > 0) else 0.0)
                    spread_pct = (ask - bid) / ask * 100.0 if ask > 0 else 999.0
                    if c.get("open_interest", c.get("oi", 0)) and spread_pct <= max_spread_pct:
                        if abs(_safe_float(c.get("strike")) - _safe_float(snap["price"])) <= strikes_width:
                            if int(c.get("dte", 999)) <= max_dte:
                                c["mid"] = mid
                                filtered.append(c)

            if len(filtered) == 0:
                logs.append(f"[{symbol}] ⚠ No liquid near-money contracts after filters.")
                continue

            # ------- Compute expected change & ROI via Greeks
            enriched: List[Dict[str, Any]] = []
            for c in filtered:
                mid = _safe_float(c.get("mid"))
                if mid <= 0:
                    continue

                delta = _safe_float(c.get("delta"))
                gamma = _safe_float(c.get("gamma"))
                theta_day = _safe_float(c.get("theta_day") or c.get("theta"))  # per day preferred
                vega = _safe_float(c.get("vega"))
                rho = _safe_float(c.get("rho"))

                # direction-aware dS (put: negative if we expect down move, call: positive)
                typ = str(c.get("type", "")).lower()
                dS_dir = dS if typ == "call" else (-dS if typ == "put" else dS)

                pnl = _expected_price_change_greeks(
                    delta=delta,
                    gamma=gamma,
                    theta_day=theta_day,
                    vega=vega,
                    rho=rho,
                    dS=dS_dir,
                    dIV_pts=dIV_pts,
                    horizon_hours=horizon_hours,
                    dRate=0.0,
                )
                roi = (pnl / mid) * 100.0

                row = dict(c)  # copy
                row.update(
                    {
                        "symbol": symbol,
                        "exp_dS": round(dS, 3),
                        "exp_dIV_pts": round(dIV_pts, 3),
                        "horizon_h": horizon_hours,
                        "exp_change": round(pnl, 2),
                        "exp_roi": round(roi, 2),
                    }
                )
                enriched.append(row)

            if len(enriched) == 0:
                logs.append(f"[{symbol}] ⚠ Contracts filtered out after PnL calc")
                continue

            # ------- Score & keep top_k per symbol
            try:
                ranked_symbol = score_contracts(enriched)
            except Exception:
                # Fallback scorer: normalize ROI and penalize spread
                ranked_symbol = []
                max_roi = max(_safe_float(x.get("exp_roi")) for x in enriched) or 1.0
                for x in enriched:
                    spread = _safe_float(x.get("ask")) - _safe_float(x.get("bid"))
                    mid = _safe_float(x.get("mid"), 1.0)
                    spread_penalty = (spread / mid) if mid > 0 else 1.0
                    base = _safe_float(x.get("exp_roi")) / max_roi
                    x["score"] = round(max(0.0, base - 0.2 * spread_penalty), 4)
                    ranked_symbol.append(x)

            ranked_symbol.sort(key=lambda r: (_safe_float(r.get("score")), _safe_float(r.get("exp_roi"))), reverse=True)
            all_rows.extend(ranked_symbol[:top_k])

        except Exception as e_ticker:
            logs.append(f"[{symbol}] ⚠ Unexpected: {e_ticker}")
            # Keep traceback in logs for debugging without crashing the whole run
            tb = traceback.format_exc(limit=1)
            logs.append(tb.strip())
            continue

    # -------------------- Global ranking and tiering --------------------------
    if len(all_rows) == 0:
        return {"tier1": [], "tier2": [], "watch": [], "all": [], "logs": logs}

    # Global sort: score then ROI
    all_rows.sort(key=lambda r: (_safe_float(r.get("score")), _safe_float(r.get("exp_roi"))), reverse=True)

    t1, t2, watch = _tier_split(
        all_rows,
        tier1_min_score=tier1_min_score,
        tier2_min_score=tier2_min_score,
        min_roi=min_roi,
    )

    # Optionally cap email length
    cap_tier1 = int(config.get("cap_tier1", 15))
    cap_tier2 = int(config.get("cap_tier2", 25))
    cap_watch = int(config.get("cap_watch", 25))

    result = {
        "tier1": t1[:cap_tier1],
        "tier2": t2[:cap_tier2],
        "watch": watch[:cap_watch],
        "all": all_rows[: (cap_tier1 + cap_tier2 + cap_watch)],
        "logs": logs,
    }
    return result


__all__ = ["generate_ranked_ideas"]