# Trading & filtering knobs (tunable without touching logic)

CFG = {
    # Signal thresholds (looser than before)
    "rsi_buy": 40.0,         # RSI <= 40 → bullish bias (CALL)
    "rsi_sell": 60.0,        # RSI >= 60 → bearish bias (PUT)

    # MACD histogram strength (absolute)
    "macd_min": 0.03,        # smaller = more trades

    # EMA trend
    "ema_fast": 20,
    "ema_slow": 50,
    "ema_trend_min": 0.0,    # fast > slow (bull) or fast < slow (bear). Keep >= 0 for permissive.

    # Move / liquidity sanity
    "min_exp_move_pts": 0.15,    # smaller = more trades (points in underlying)
    "max_spread_pct": 0.45,      # 45% of mark
    "min_open_interest": 50,     # min OI per contract

    # Ranking weights (sum doesn’t need to be 1)
    "w_rsi": 0.35,
    "w_macd": 0.30,
    "w_trend": 0.20,
    "w_zscore": 0.15,            # push moves that are statistically stretched

    # Tiers
    "tier1_min": 0.65,           # score ≥ 0.65 → Tier 1
    "tier2_min": 0.45,           # score ≥ 0.45 → Tier 2 (else watchlist)

    # Option scan window
    "dte_min": 0,                # same day allowed
    "dte_max": 14,               # near-dated for day/swing
    "strike_window": 12.0,       # ±$ window around underlying price

    # Forecast horizon (hours)
    "default_horizon_h": 2,
}