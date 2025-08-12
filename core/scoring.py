import numpy as np

def _norm_clip(x, lo, hi):
    return float(np.clip((x - lo) / (hi - lo), 0.0, 1.0))

def score_signal(snap: dict, z_abs: float, cfg: dict, direction: str) -> float:
    """
    Build a 0..1 score from RSI, MACD hist, EMA trend, and |zscore|.
    Direction-aware: for CALL, lower RSI is better; for PUT, higher RSI is better.
    """
    w_rsi   = cfg["w_rsi"]
    w_macd  = cfg["w_macd"]
    w_trend = cfg["w_trend"]
    w_z     = cfg["w_zscore"]

    rsi = snap["rsi"]
    macd = snap["macd_hist"]
    trend = snap["ema_trend"]

    # RSI component
    if direction == "CALL":
        rsi_component = _norm_clip(60.0 - rsi, 0.0, 30.0)  # RSI 30 → 1.0, 60 → 0.0
    else:
        rsi_component = _norm_clip(rsi - 40.0, 0.0, 30.0)  # RSI 70 → 1.0, 40 → 0.0

    # MACD strength (favor sign aligned with direction)
    macd_signed = macd if direction == "CALL" else -macd
    macd_component = _norm_clip(macd_signed, 0.0, 0.10)  # 0..0.1 typical hist range

    # Trend (EMA fast-slow) aligned with direction
    trend_signed = trend if direction == "CALL" else -trend
    trend_component = _norm_clip(trend_signed, 0.0, max(0.01, abs(trend_signed) * 2.0))

    # Zscore of recent move (stretched -> reversal or continuation bias)
    z_component = _norm_clip(z_abs, 0.5, 2.0)

    score = (
        w_rsi * rsi_component +
        w_macd * macd_component +
        w_trend * trend_component +
        w_z * z_component
    )

    return float(np.clip(score, 0.0, 1.0))