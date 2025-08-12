# core/signals.py
from typing import List, Dict, Any, Tuple
from datetime import datetime, timezone

from core.forecast import forecast_move              # you already have this
from core.options import get_atm_options             # you already have this
from core.greeks import bs_price_greeks              # fallback when greeks missing
from core.features import build_features             # NEW (from previous step)
from core.scoring import score_contract              # NEW (from previous step)

# ---------- tiny utils ----------
def _mid_price(bid: float, ask: float, last_price: float) -> float:
    try:
        b = float(bid); a = float(ask)
        if b > 0 and a > 0:
            return round((b + a) / 2.0, 4)
    except Exception:
        pass
    try:
        return round(float(last_price or 0.0), 4)
    except Exception:
        return 0.0

def _to_years(expiration_str: str) -> float:
    try:
        exp = datetime.fromisoformat(str(expiration_str).split("T")[0]).replace(tzinfo=timezone.utc)
    except Exception:
        return 0.0
    days = (exp - datetime.utcnow().replace(tzinfo=timezone.utc)).days
    return max(days, 0) / 365.0

def _fill_greeks_if_missing(opt: Dict[str, Any], S: float, r: float = 0.05) -> Dict[str, Any]:
    """
    Use Yahoo greeks if present; otherwise compute via BS using the row IV.
    Conventions:
      - theta: per day
      - vega : per +1 vol point (0.01)
    """
    delta = float(opt.get("delta") or 0.0)
    gamma = float(opt.get("gamma") or 0.0)
    theta = float(opt.get("theta") or 0.0)
    vega  = float(opt.get("vega") or 0.0)

    iv = float(opt.get("impliedVolatility") or 0.0)  # decimal
    K  = float(opt.get("strike") or 0.0)
    T  = _to_years(str(opt.get("expiration", "")))
    typ= "call" if str(opt.get("type","CALL")).upper() == "CALL" else "put"

    # recompute if any key greek looks unusable
    if (abs(delta) < 0.01 or gamma == 0.0 or theta == 0.0 or vega == 0.0) and (iv > 0 and T > 0 and K > 0):
        bs = bs_price_greeks(S, K, T, r, iv, typ)
        delta = bs["delta"]
        gamma = bs["gamma"]
        theta = bs["theta_per_day"]
        vega  = bs["vega_per_1pct"]

    opt["delta"] = float(delta)
    opt["gamma"] = float(gamma)
    opt["theta"] = float(theta)
    opt["vega"]  = float(vega)
    return opt

def _taylor_change(opt: Dict[str, Any], dS: float, dSigma_pts: float, days_forward: float) -> float:
    delta = float(opt.get("delta", 0.0))
    gamma = float(opt.get("gamma", 0.0))
    theta = float(opt.get("theta", 0.0))     # per day
    vega  = float(opt.get("vega", 0.0))      # per +1 vol point
    return (delta * dS) + (0.5 * gamma * dS * dS) + (theta * days_forward) + (vega * dSigma_pts)

def _detect_regime_utc() -> Tuple[int,int,int]:
    """
    Lightweight regime detection using UTC time.
    US equities open 13:30 UTC (8:30 CT during DST).
    open:   13:30–15:00 UTC
    midday: 15:00–19:30 UTC
    close:  19:30–21:00 UTC
    Returns three flags: (open, midday, close)
    """
    now = datetime.utcnow()
    hhmm = now.hour * 100 + now.minute
    open_f   = 1330 <= hhmm < 1500
    close_f  = 1930 <= hhmm < 2100
    midday_f = not open_f and not close_f
    return (1 if open_f else 0, 1 if midday_f else 0, 1 if close_f else 0)

# ---------- main engine ----------
def generate_ranked_ideas(
    tickers: List[str],
    horizon_hours: int = 2,
    iv_change_pts: float = 0.5,
    min_score_tier1: float = 80.0,
    min_score_tier2: float = 60.0,
    dte_min: int = 0,
    dte_max: int = 14,
    strikes_range: int = 8,
    topN_fallback: int = 5,
    min_oi: int = 100,
    max_spread_pct: float = 0.35
) -> Dict[str, Any]:
    """
    Returns a dict:
      {
        "tier1": [ideas...], "tier2": [ideas...], "watch": [ideas...],
        "all": [ideas_sorted...], "logs": ["[SPY] stats: ...", ...]
      }
    Each idea has fields: Ticker, Type, Strike, Expiration, Buy Price, Sell Price, ROI, Score, Reasons, etc.
    """
    days_forward = float(horizon_hours) / 24.0
    reg_open, reg_mid, reg_close = _detect_regime_utc()

    out_all: List[Dict[str, Any]] = []
    logs: List[str] = []

    for t in tickers:
        # 1) forecast underlying move
        f = forecast_move(t, horizon_hours=horizon_hours, bias_mode="revert")
        if not f.get("ok"):
            logs.append(f"[{t}] ⚠️ Forecast issue: {f.get('reason')}")
            continue

        S      = float(f["S"])
        dS_up  = float(f["dS_up"])
        dS_dn  = float(f["dS_dn"])
        gap    = float(f.get("gap_pct", 0.0))
        m1h    = float(f.get("mom_1h", 0.0))
        m3h    = float(f.get("mom_3h", 0.0))
        iv1d   = float(f.get("iv_1d_chg_pts", 0.0))
        ivp30  = float(f.get("iv_percentile_30d", 0.5))

        # 2) pull option candidates around ATM
        chain = get_atm_options(
            t,
            max_dte=dte_max,
            min_dte=dte_min,
            strikes_range=strikes_range
        )
        if not chain:
            logs.append(f"[{t}] ⚠️ No option data.")
            continue

        # stats counters
        stats = {"mid<=0":0, "missing_iv":0, "thin_oi":0, "wide_spread":0, "ok":0}

        for opt in chain:
            side = str(opt.get("type","CALL")).upper()
            iv   = float(opt.get("impliedVolatility") or 0.0)
            if iv <= 0.0:
                stats["missing_iv"] += 1
                continue

            bid = float(opt.get("bid") or 0.0)
            ask = float(opt.get("ask") or 0.0)
            last= float(opt.get("lastPrice") or 0.0)
            mid = _mid_price(bid, ask, last)
            if mid <= 0:
                stats["mid<=0"] += 1
                continue

            # Liquidity guards
            oi  = int(opt.get("openInterest") or 0)
            spr = ((ask - bid) / mid) if (mid > 0 and ask > 0 and bid > 0) else 1.0
            if oi < min_oi:
                stats["thin_oi"] += 1
                continue
            if spr >= max_spread_pct:
                stats["wide_spread"] += 1
                continue

            # Fill greeks if Yahoo gave zeros
            opt = _fill_greeks_if_missing(opt, S=S, r=0.05)

            # 3) compute Taylor ΔOption and ROI
            dS = dS_up if side == "CALL" else dS_dn
            dOpt = _taylor_change(opt, dS=dS, dSigma_pts=iv_change_pts, days_forward=days_forward)
            if dOpt <= 0:
                continue

            roi_pct = 100.0 * (dOpt / mid)

            # 4) build features & score
            market_ctx = {
                "S": S,
                "gap_pct": gap,
                "mom_1h": m1h,
                "mom_3h": m3h,
                "regime_open": reg_open,
                "regime_midday": reg_mid,
                "regime_close": reg_close,
                "iv_change_pts": iv_change_pts,
                "iv_1d_chg_pts": iv1d,
                "iv_percentile_30d": ivp30,
            }
            feat = build_features(option_row=opt, market_ctx=market_ctx, roi_pct=roi_pct, expected_change=dOpt)
            score, reasons = score_contract(feat)

            stats["ok"] += 1

            out_all.append({
                "Ticker": t,
                "Type": "Call" if side == "CALL" else "Put",
                "Strike": float(opt.get("strike")),
                "Expiration": str(opt.get("expiration")),
                "Spot": S,
                "Buy Price": round(mid, 4),
                "Sell Price": round(mid + dOpt, 4),
                "Expected Change": round(dOpt, 4),
                "ROI": round(roi_pct, 2),
                "Score": round(score, 1),
                "Reasons": reasons,
                "DTE": int(opt.get("DTE", 0)),
                "IV": round(iv, 4),
                "Delta": round(float(opt.get("delta", 0.0)), 4),
                "Gamma": round(float(opt.get("gamma", 0.0)), 4),
                "Theta": round(float(opt.get("theta", 0.0)), 4),
                "Vega":  round(float(opt.get("vega", 0.0)), 4),
            })

        logs.append(f"[{t}] stats: " + ", ".join(f"{k}={v}" for k,v in stats.items()))

    # 5) rank & tier
    out_all.sort(key=lambda x: x["Score"], reverse=True)

    tier1 = [x for x in out_all if x["Score"] >= min_score_tier1]
    tier2 = [x for x in out_all if min_score_tier2 <= x["Score"] < min_score_tier1]
    watch = [x for x in out_all if x["Score"] < min_score_tier2][:max(0, topN_fallback - len(tier1) - len(tier2))]

    # fallback: if nothing in tier1/tier2, send topN anyway
    if not tier1 and not tier2 and out_all:
        watch = out_all[:topN_fallback]

    return {
        "tier1": tier1,
        "tier2": tier2,
        "watch": watch,
        "all": out_all,
        "logs": logs
    }