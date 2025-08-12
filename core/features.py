# core/features.py
from typing import Dict, Any

def _nz(x, default=0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def _mid_price(bid: float, ask: float, last_price: float) -> float:
    try:
        b = float(bid); a = float(ask)
        if b > 0 and a > 0:
            return round((b + a) / 2.0, 4)
    except Exception:
        pass
    return round(_nz(last_price, 0.0), 4)

def _spread_pct(bid: float, ask: float, mid: float) -> float:
    try:
        b = float(bid); a = float(ask); m = float(mid)
        if b > 0 and a > 0 and m > 0:
            return max((a - b) / m, 0.0)
    except Exception:
        pass
    return 1.0  # unknown -> very wide

def _scale_gamma(gamma: float, spot: float) -> float:
    """Map gamma to ~0..1 for scoring (heuristic)."""
    g = abs(_nz(gamma, 0.0)) * max(spot, 1.0) / 50.0
    return max(0.0, min(g, 1.0))

def _theta_penalty_per_day(theta: float, price: float) -> float:
    """Positive penalty 0..1 measuring theta pain vs price."""
    p = max(price, 0.05)
    pen = -_nz(theta, 0.0) / p  # neg theta => positive penalty
    return max(0.0, min(pen, 1.0))

def build_features(
    option_row: Dict[str, Any],
    market_ctx: Dict[str, Any],
    roi_pct: float,
    expected_change: float = None
) -> Dict[str, float]:
    """
    Create the feature dictionary the scorer expects.

    option_row: raw option row (Yahoo-style) incl. greeks if present.
    market_ctx: context from forecast/regime detection.
    roi_pct:    Taylor-estimated % return over the chosen horizon.
    expected_change: absolute Δ option price (diagnostic).
    """
    # market context
    S      = _nz(market_ctx.get("S"), 0.0)
    gap    = _nz(market_ctx.get("gap_pct"), 0.0)
    m1h    = _nz(market_ctx.get("mom_1h"), 0.0)
    m3h    = _nz(market_ctx.get("mom_3h"), 0.0)
    r_open = 1.0 if _nz(market_ctx.get("regime_open"), 0.0) > 0 else 0.0
    r_mid  = 1.0 if _nz(market_ctx.get("regime_midday"), 0.0) > 0 else 0.0
    r_close= 1.0 if _nz(market_ctx.get("regime_close"), 0.0) > 0 else 0.0
    dSigma = _nz(market_ctx.get("iv_change_pts"), 0.0)
    iv1d   = _nz(market_ctx.get("iv_1d_chg_pts"), 0.0)
    ivp30  = _nz(market_ctx.get("iv_percentile_30d"), 0.5)

    # option row
    bid  = _nz(option_row.get("bid"), 0.0)
    ask  = _nz(option_row.get("ask"), 0.0)
    last = _nz(option_row.get("lastPrice"), 0.0)
    mid  = _mid_price(bid, ask, last)
    spr  = _spread_pct(bid, ask, mid)

    iv    = _nz(option_row.get("impliedVolatility"), 0.0)  # decimal
    delta = _nz(option_row.get("delta"), 0.0)
    gamma = _nz(option_row.get("gamma"), 0.0)
    theta = _nz(option_row.get("theta"), 0.0)  # per day
    vega  = _nz(option_row.get("vega"), 0.0)   # per +1 vol pt

    oi    = _nz(option_row.get("openInterest"), 0.0)
    vol   = _nz(option_row.get("volume"), 0.0)

    # derived / scaled
    delta_abs   = abs(delta)
    gamma_s     = _scale_gamma(gamma, S)
    theta_pen   = _theta_penalty_per_day(theta, mid)
    vega_benef  = (vega * dSigma) / max(mid, 0.05)
    vega_benef  = max(-0.5, min(vega_benef, 0.5))  # clamp

    feat = {
        # payoff/greeks
        "roi_pct": float(roi_pct),
        "delta_abs": delta_abs,
        "gamma_s": gamma_s,
        "theta_penalty": theta_pen,
        "vega_iv_benefit": vega_benef,

        # underlying/regime
        "gap_pct": gap,
        "mom_1h": m1h,
        "mom_3h": m3h,
        "regime_open": r_open,
        "regime_midday": r_mid,
        "regime_close": r_close,

        # liquidity / tradability
        "oi": oi,
        "vol": vol,
        "spread_pct": spr,
        "price": mid,

        # IV context
        "iv": iv,
        "iv_1d_chg_pts": iv1d,
        "iv_percentile_30d": ivp30,
    }

    if expected_change is not None:
        try:
            feat["exp_change_abs"] = float(expected_change)
        except Exception:
            pass

    return feat