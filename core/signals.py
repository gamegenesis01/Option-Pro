# core/signals.py
from typing import List, Dict, Any
from datetime import datetime
from core.forecast import forecast_move
from core.options import get_atm_options
from core.greeks import bs_price_greeks

def _mid_price(bid: float, ask: float, last_price: float) -> float:
    try:
        b = float(bid)
        a = float(ask)
        if b > 0 and a > 0:
            return round((b + a) / 2.0, 4)
    except Exception:
        pass
    return round(float(last_price or 0.0), 4)

def _to_years(expiration_str: str) -> float:
    # expiration like '2025-09-12' or ISO with time
    try:
        exp = datetime.strptime(expiration_str.split("T")[0], "%Y-%m-%d")
    except Exception:
        return 0.0
    days = (exp - datetime.utcnow()).days
    return max(days, 0) / 365.0

def _fill_greeks_if_missing(opt: Dict[str, Any], S: float, r: float = 0.05) -> Dict[str, Any]:
    """
    Use greeks from Yahoo if they look valid; otherwise compute via Black–Scholes.
    We expect:
      - theta as *per day*
      - vega  as *per +1 vol point (0.01)*
    """
    delta = float(opt.get("delta") or 0.0)
    gamma = float(opt.get("gamma") or 0.0)
    theta = float(opt.get("theta") or 0.0)   # per day (Yahoo convention)
    vega  = float(opt.get("vega") or 0.0)    # per 1 vol-pt
    iv    = float(opt.get("impliedVolatility") or 0.0)  # decimal (e.g., 0.25)
    K     = float(opt["strike"])
    T_y   = _to_years(str(opt["expiration"]))
    typ   = "call" if str(opt["type"]).upper() == "CALL" else "put"

    # Heuristic: if |delta| small or any of gamma/theta/vega ~ 0, recompute
    need_bs = (abs(delta) < 0.01) or (gamma == 0.0) or (theta == 0.0) or (vega == 0.0)
    if need_bs and iv > 0 and T_y > 0:
        bs = bs_price_greeks(S, K, T_y, r, iv, typ)
        delta = bs["delta"]
        gamma = bs["gamma"]               # per $^2
        theta = bs["theta_per_day"]       # per day
        vega  = bs["vega_per_1pct"]       # per +1 vol point

    opt["delta"] = float(delta)
    opt["gamma"] = float(gamma)
    opt["theta"] = float(theta)
    opt["vega"]  = float(vega)
    return opt

def taylor_change(opt: Dict[str, Any], dS: float, dSigma_pts: float, days_forward: float) -> float:
    """
    Taylor expansion using option greeks.
      - theta is *per day*
      - vega is *per +1 vol point (0.01)*
      - rho ignored intraday (Δr = 0)
    """
    delta = float(opt.get("delta", 0.0))
    gamma = float(opt.get("gamma", 0.0))
    theta = float(opt.get("theta", 0.0))
    vega  = float(opt.get("vega", 0.0))

    return (
        (delta * dS) +
        (0.5 * gamma * (dS ** 2)) +
        (theta * days_forward) +
        (vega  * dSigma_pts)
    )

def generate_trade_ideas(
    tickers: List[str],
    horizon_hours: int = 2,
    iv_change_pts: float = 0.0,   # +0.5 means +0.5 vol-pt assumption
    min_roi_pct: float = 12.0,
    dte_min: int = 1,             # include short-dated
    dte_max: int = 14,            # allow a bit wider window
    strikes_range: int = 5        # widen vs ±$2
) -> List[Dict[str, Any]]:
    """
    1) Forecast dS magnitude from hourly realized vol.
    2) Pull near-term options around ATM.
    3) Estimate ΔOption with Taylor expansion.
    4) Keep where ROI >= min_roi_pct.
    """
    ideas: List[Dict[str, Any]] = []
    days_forward = float(horizon_hours) / 24.0

    for t in tickers:
        try:
            f = forecast_move(t, horizon_hours=horizon_hours, bias_mode="revert")
            if not f.get("ok"):
                print(f"[{t}] ⚠️ Forecast issue: {f.get('reason')}")
                continue

            S = float(f["S"])
            dS_up = float(f["dS_up"])
            dS_dn = float(f["dS_dn"])

            chain = get_atm_options(
                t,
                max_dte=dte_max,
                min_dte=dte_min,
                strikes_range=strikes_range
            )
            if not chain:
                print(f"[{t}] ⚠️ No option data.")
                continue

            reasons = {"mid<=0": 0, "missing_iv": 0, "dOpt<=0": 0, "roi<thresh": 0, "ok": 0}

            for opt in chain:
                side = str(opt["type"]).upper()  # "CALL"/"PUT"
                iv = float(opt.get("impliedVolatility") or 0.0)
                if iv <= 0.0:
                    reasons["missing_iv"] += 1
                    continue

                mid = _mid_price(opt.get("bid", 0.0), opt.get("ask", 0.0), opt.get("lastPrice", 0.0))
                if mid <= 0:
                    reasons["mid<=0"] += 1
                    continue

                # Fill greeks if Yahoo returned zeros
                opt = _fill_greeks_if_missing(opt, S=S, r=0.05)

                # Direction: Calls favor up, Puts favor down
                dS = dS_up if side == "CALL" else dS_dn

                dOpt = taylor_change(opt, dS=dS, dSigma_pts=iv_change_pts, days_forward=days_forward)
                if dOpt <= 0:
                    reasons["dOpt<=0"] += 1
                    continue

                roi_pct = 100.0 * (dOpt / mid)
                if roi_pct < min_roi_pct:
                    reasons["roi<thresh"] += 1
                    continue

                reasons["ok"] += 1

                ideas.append({
                    "Ticker": t,
                    "Type": "Call" if side == "CALL" else "Put",
                    "Strike": float(opt["strike"]),
                    "Expiration": str(opt["expiration"]),
                    "Spot": S,
                    "Buy Price": round(mid, 4),
                    "Expected Change": round(dOpt, 4),
                    "Sell Price": round(mid + dOpt, 4),
                    "ROI": round(roi_pct, 2),
                    "DTE": int(opt["DTE"]),
                    "IV": round(iv, 4),
                    "Delta": round(float(opt.get("delta", 0.0)), 4),
                    "Gamma": round(float(opt.get("gamma", 0.0)), 4),
                    "Theta": round(float(opt.get("theta", 0.0)), 4),
                    "Vega":  round(float(opt.get("vega", 0.0)), 4),
                    "Assumptions": {"HorizonHours": horizon_hours, "dSigmaPts": iv_change_pts}
                })

            print(f"[{t}] stats: " + ", ".join(f"{k}={v}" for k, v in reasons.items()))
        except Exception as e:
            print(f"[{t}] ⚠️ Error: {e}")

    return ideas
