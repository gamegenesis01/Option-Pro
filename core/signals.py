from typing import List, Dict, Any, Tuple
import numpy as np
import pandas as pd
from .strategy import CFG
from .fetch_data import get_price_history
from .features import add_features, latest_snapshot
from .forecast import forecast_move
from .options import get_option_chain_near_money
from .filter_options import filter_chain_liquidity
from .scoring import score_signal

def _two_of_three_triggers(snap: dict, cfg: dict) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Decide direction using 3 signals (need >=2 confluences):
      1) RSI (<= rsi_buy → CALL, >= rsi_sell → PUT)
      2) MACD hist sign (>= macd_min → CALL, <= -macd_min → PUT)
      3) EMA trend sign (fast-slow >= ema_trend_min → CALL, <= -ema_trend_min → PUT)
    """
    rsi_buy, rsi_sell = cfg["rsi_buy"], cfg["rsi_sell"]
    macd_min = cfg["macd_min"]
    trend_min = cfg["ema_trend_min"]

    votes_call = 0
    votes_put  = 0
    reasons = []

    # RSI vote
    if snap["rsi"] <= rsi_buy:
        votes_call += 1
        reasons.append(f"RSI≤{rsi_buy}")
    elif snap["rsi"] >= rsi_sell:
        votes_put += 1
        reasons.append(f"RSI≥{rsi_sell}")

    # MACD vote
    if snap["macd_hist"] >= macd_min:
        votes_call += 1
        reasons.append(f"MACD≥{macd_min:.2f}")
    elif snap["macd_hist"] <= -macd_min:
        votes_put += 1
        reasons.append(f"MACD≤{-macd_min:.2f}")

    # EMA trend vote
    if snap["ema_trend"] >= trend_min:
        votes_call += 1
        reasons.append(f"EMA↑≥{trend_min}")
    elif snap["ema_trend"] <= -trend_min:
        votes_put += 1
        reasons.append(f"EMA↓≤{-trend_min}")

    direction = None
    if votes_call >= 2 and votes_call > votes_put:
        direction = "CALL"
    elif votes_put >= 2 and votes_put > votes_call:
        direction = "PUT"

    return (direction is not None), (direction or "NONE"), {
        "votes_call": votes_call, "votes_put": votes_put, "reasons": reasons
    }

def _expected_move_pts(snap: dict, z_abs: float, cfg: dict) -> float:
    # Blend ATR and zscore to propose a modest expected move
    atr = max(1e-9, float(snap["atr"]))
    base = 0.40 * atr + 0.10 * z_abs  # small, realistic for 1–3h
    return max(cfg["min_exp_move_pts"], round(base, 2))

def generate_ranked_ideas(
    tickers: List[str],
    horizon_hours: int = None,
    cfg: dict = None
) -> Dict[str, Any]:
    """
    Main entry: returns dict with 'tier1', 'tier2', 'watch', 'logs'.
    """
    cfg = dict(CFG) if cfg is None else cfg
    horizon_h = horizon_hours or cfg["default_horizon_h"]

    tier1, tier2, watch = [], [], []
    logs = []

    for t in tickers:
        try:
            # 1) Data & features
            df = get_price_history(t, period="30d", interval="60m")  # 1h bars per your request
            if df is None or df.empty:
                logs.append((t, "NoData"))
                continue

            fdf = add_features(df, ema_fast=cfg["ema_fast"], ema_slow=cfg["ema_slow"])
            snap = latest_snapshot(fdf)

            # 2) Quick check: confluence decision
            ok, direction, meta = _two_of_three_triggers(snap, cfg)
            if not ok:
                logs.append((t, "NoConfluence"))
                continue

            # 3) Stats-based move (zscore etc.)
            fc = forecast_move(t, horizon_hours=horizon_h, bias_mode="revert")
            z_abs = abs(float(fc.get("zscore", 0.0)))

            exp_move = _expected_move_pts(snap, z_abs, cfg)

            # 4) Pull options, keep near-the-money, DTE 0..14
            chain = get_option_chain_near_money(
                ticker=t,
                direction=direction,
                strike_window=cfg["strike_window"],
                dte_min=cfg["dte_min"],
                dte_max=cfg["dte_max"]
            )
            if chain.empty:
                logs.append((t, "NoChain"))
                continue

            chain = filter_chain_liquidity(
                chain,
                max_spread_pct=cfg["max_spread_pct"],
                min_open_interest=cfg["min_open_interest"]
            )
            if chain.empty:
                logs.append((t, "Illiquid"))
                continue

            # 5) Score & choose best contract
            score = score_signal(snap, z_abs, cfg, direction)
            best = chain.sort_values("spread_pct").iloc[0].to_dict()

            idea = {
                "ticker": t,
                "direction": direction,
                "reason": ", ".join(meta["reasons"]),
                "score": round(score, 3),
                "price": round(snap["close"], 2),
                "exp_move_pts": exp_move,
                "contract": {
                    "symbol": best.get("contractSymbol"),
                    "dte": int(best.get("dte", 0)),
                    "strike": float(best.get("strike", 0.0)),
                    "mark": round(float(best.get("mark", 0.0)), 2),
                    "spread_pct": round(float(best.get("spread_pct", 0.0)), 2),
                    "oi": int(best.get("openInterest", 0)),
                },
                "debug": {
                    "rsi": round(snap["rsi"], 2),
                    "macd_hist": round(snap["macd_hist"], 4),
                    "ema_trend": round(snap["ema_trend"], 4),
                    "zscore": round(z_abs, 2),
                }
            }

            if score >= cfg["tier1_min"]:
                tier1.append(idea)
            elif score >= cfg["tier2_min"]:
                tier2.append(idea)
            else:
                watch.append(idea)

            logs.append((t, "OK"))

        except Exception as e:
            logs.append((t, f"ERR:{e}"))

    # Rank inside buckets by score desc, then smallest spread
    def _sort(bucket):
        return sorted(bucket, key=lambda x: (-x["score"], x["contract"]["spread_pct"]))

    return {
        "tier1": _sort(tier1),
        "tier2": _sort(tier2),
        "watch": _sort(watch),
        "logs": logs,
        "meta": {
            "horizon_h": horizon_h,
            "cfg": {
                "rsi": (cfg["rsi_buy"], cfg["rsi_sell"]),
                "macd_min": cfg["macd_min"],
                "ema_fast": cfg["ema_fast"],
                "ema_slow": cfg["ema_slow"],
                "max_spread_pct": cfg["max_spread_pct"],
                "min_open_interest": cfg["min_open_interest"],
                "dte": (cfg["dte_min"], cfg["dte_max"]),
            }
        }
    }