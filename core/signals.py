# core/signals.py
from typing import List, Dict, Any
from core.forecast import forecast_move
from core.options import get_atm_options

def _mid_price(bid: float, ask: float, last_price: float) -> float:
    if bid and ask and bid > 0 and ask > 0:
        return round((bid + ask) / 2.0, 4)
    return round(float(last_price), 4)

def taylor_change(opt: Dict[str, Any], dS: float, dSigma_pts: float, days_forward: float) -> float:
    """
    Taylor expansion using option's greeks as provided by yfinance.
    Assumptions:
      - theta is per DAY
      - vega is per +1 vol point (0.01)
      - rho ignored intraday (Δr=0)
    Inputs:
      dS           : underlying $ move over horizon (can be + or -)
      dSigma_pts   : change in IV in vol points (e.g., +1.0 means +0.01)
      days_forward : horizon in DAYS (e.g., 2 hours = 2/24)
    """
    delta = float(opt.get("delta", 0.0))
    gamma = float(opt.get("gamma", 0.0))
    theta = float(opt.get("theta", 0.0))         # per day
    vega  = float(opt.get("vega", 0.0))          # per 1 vol-pt (0.01)
    # rho ignored intraday

    return (
        (delta * dS) +
        (0.5 * gamma * (dS ** 2)) +
        (theta * days_forward) +
        (vega  * dSigma_pts)
    )

def generate_trade_ideas(
    tickers: List[str],
    horizon_hours: int = 2,
    iv_change_pts: float = 0.0,   # assume flat IV by default (set 1.0 for +1pt)
    min_roi_pct: float = 12.0,    # only alert if ROI >= this
    dte_min: int = 2,
    dte_max: int = 10,
    strikes_range: int = 2
) -> List[Dict[str, Any]]:
    """
    For each ticker:
      1) Forecast hourly move magnitude using realized vol (1h bars).
      2) Get near-term options around ATM (2–10 DTE by default).
      3) Use Taylor expansion with option greeks to estimate Δ option price over horizon.
      4) Keep ideas where ROI meets threshold.

    Returns a list of dicts ready for email.
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

            chain = get_atm_options(t, max_dte=dte_max, min_dte=dte_min, strikes_range=strikes_range)
            if not chain:
                print(f"[{t}] ⚠️ No option data.")
                continue

            for opt in chain:
                side = opt["type"]  # "CALL" or "PUT"
                mid = _mid_price(opt["bid"], opt["ask"], opt["lastPrice"])
                if mid <= 0:
                    continue

                # Calls benefit from up move; Puts from down move
                dS = dS_up if side == "CALL" else dS_dn
                dOpt = taylor_change(opt, dS=dS, dSigma_pts=iv_change_pts, days_forward=days_forward)

                if dOpt <= 0:
                    continue

                roi_pct = 100.0 * (dOpt / mid)
                if roi_pct < min_roi_pct:
                    continue

                sell_est = round(mid + dOpt, 4)

                ideas.append({
                    "Ticker": t,
                    "Type": side.title(),
                    "Strike": float(opt["strike"]),
                    "Expiration": str(opt["expiration"]),
                    "Spot": S,
                    "Buy Price": round(mid, 4),
                    "Expected Change": round(dOpt, 4),
                    "Sell Price": sell_est,
                    "ROI": round(roi_pct, 2),
                    "DTE": int(opt["DTE"]),
                    "IV": round(float(opt["impliedVolatility"]), 4),
                    "Delta": round(float(opt.get("delta", 0.0)), 4),
                    "Gamma": round(float(opt.get("gamma", 0.0)), 4),
                    "Theta": round(float(opt.get("theta", 0.0)), 4),
                    "Vega":  round(float(opt.get("vega", 0.0)), 4),
                    "Assumptions": {
                        "HorizonHours": horizon_hours,
                        "dSigmaPts": iv_change_pts
                    }
                })

        except Exception as e:
            print(f"[{t}] ⚠️ Error: {e}")

    return ideas
