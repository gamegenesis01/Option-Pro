# core/signals.py
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from core.fetch_data import get_price_history


# ---------- utils

def _clean_close_series(px: pd.DataFrame) -> pd.Series:
    """
    Ensure we return a numeric, 1-D close Series with NaNs dropped.
    Raises ValueError if unusable.
    """
    if px is None or not isinstance(px, pd.DataFrame) or px.empty:
        raise ValueError("empty_frame")

    # Accept several common column spellings
    for c in ("close", "Close", "adj_close", "Adj Close"):
        if c in px.columns:
            s = pd.to_numeric(px[c], errors="coerce").dropna()
            if s.empty:
                raise ValueError("no_valid_close")
            return s

    raise ValueError("no_close_column")


def _rsi(series: pd.Series, period: int = 14) -> float:
    """Simple RSI implementation that works on a 1‑D Series."""
    if len(series) < period + 1:
        return float("nan")
    delta = series.diff()
    up = delta.clip(lower=0.0)
    down = -delta.clip(upper=0.0)
    # Wilder's smoothing
    roll_up = up.ewm(alpha=1 / period, adjust=False).mean()
    roll_down = down.ewm(alpha=1 / period, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])


def _momentum(series: pd.Series, lookback: int) -> float:
    """Percent change over lookback bars."""
    if len(series) <= lookback:
        return float("nan")
    old = float(series.iloc[-lookback])
    new = float(series.iloc[-1])
    if not math.isfinite(old) or old == 0:
        return float("nan")
    return (new - old) / old


def _vol(series: pd.Series, win: int = 20) -> float:
    """Simple realized volatility proxy (std of returns)."""
    if len(series) < win + 1:
        return float("nan")
    rets = series.pct_change().dropna()
    if rets.empty:
        return float("nan")
    return float(rets.rolling(win).std().iloc[-1])


# ---------- ranking logic (simple placeholder signals)

@dataclass
class SignalSnapshot:
    symbol: str
    last: float
    rsi: float
    m1h: float
    vol20: float
    score: float
    note: str = ""


def _score_snapshot(s: SignalSnapshot) -> float:
    """
    Combine signals into a single score.
    Tuned to be stable and monotonic; adjust as you like.
    """
    score = 0.0

    # Momentum helps; volatility penalizes; RSI mean-revert if extreme
    if math.isfinite(s.m1h):
        score += 100 * s.m1h  # scale momentum
    if math.isfinite(s.vol20):
        score -= 10 * s.vol20  # penalize choppy names

    if math.isfinite(s.rsi):
        # Mild premium if RSI is mid (not overbought/oversold),
        # discount if very high/low (potential fade)
        score -= max(0.0, s.rsi - 70) * 0.5
        score -= max(0.0, 30 - s.rsi) * 0.5

    return float(score)


# ---------- public API

def generate_ranked_ideas(config: Dict) -> Dict[str, List[Dict]]:
    """
    Build ranked idea buckets.

    config keys expected (keep loose on purpose):
      - tickers: List[str]
      - horizon_hours: int  (used only to size the pull; not critical)
      - px_interval: str    (e.g., '5m', '15m', '1h' — passed through)
      - min_bars: int       (safety; default 100)
    Returns:
      { "tier1": [...], "tier2": [...], "watch": [...], "logs": [...] }
    """
    tickers: List[str] = config.get("tickers", [])
    px_interval: str = config.get("px_interval", "5m")
    horizon_hours: int = int(config.get("horizon_hours", 6))
    min_bars: int = int(config.get("min_bars", 100))

    # Pull a few extra hours to make sure we have enough bars
    pull_hours = max(horizon_hours, 6) + 24

    tier1: List[Dict] = []
    tier2: List[Dict] = []
    watch: List[Dict] = []
    logs: List[str] = []

    snapshots: List[SignalSnapshot] = []

    for symbol in tickers:
        try:
            # Fetch prices
            px = get_price_history(symbol, interval=px_interval, hours=pull_hours)

            # Always reduce to a 1-D close series
            closes = _clean_close_series(px)

            if len(closes) < min_bars:
                logs.append(f"- [{symbol}] ⚠ Forecast issue: too_few_bars ({len(closes)})")
                continue

            # Make sure latest price is finite
            last = float(closes.iloc[-1])
            if not math.isfinite(last):
                logs.append(f"- [{symbol}] ⚠ Forecast issue: bad_snapshot (invalid last close)")
                continue

            # Features computed off the 1‑D close series only
            # map 1h momentum to bars depending on interval
            interval_to_minutes = {
                "1m": 1, "2m": 2, "5m": 5, "10m": 10, "15m": 15, "30m": 30,
                "60m": 60, "1h": 60
            }
            minutes = interval_to_minutes.get(px_interval, 5)
            lookback_bars_1h = max(1, 60 // max(1, minutes))  # e.g., 12 bars for 5m

            rsi = _rsi(closes, period=14)
            m1h = _momentum(closes, lookback_bars_1h)
            vol20 = _vol(closes, win=20)

            snap = SignalSnapshot(
                symbol=symbol,
                last=last,
                rsi=rsi,
                m1h=m1h,
                vol20=vol20,
                score=0.0,
            )
            snap.score = _score_snapshot(snap)
            snapshots.append(snap)

        except ValueError as ve:
            # Our explicit validation errors
            reason = str(ve)
            if reason == "empty_frame":
                logs.append(f"- [{symbol}] ⚠ Forecast issue: empty_frame")
            elif reason == "no_valid_close":
                logs.append(f"- [{symbol}] ⚠ Forecast issue: bad_snapshot (no valid close)")
            elif reason == "no_close_column":
                logs.append(f"- [{symbol}] ⚠ Forecast issue: bad_snapshot (no close column)")
            else:
                logs.append(f"- [{symbol}] ⚠ Forecast issue: {reason}")
        except Exception as e:
            logs.append(f"- [{symbol}] ⚠ Unexpected: {type(e).__name__}: {e}")

    if not snapshots:
        # Nothing usable — return empty buckets with logs
        return {"tier1": tier1, "tier2": tier2, "watch": watch, "logs": logs}

    # Rank by score descending
    snapshots.sort(key=lambda s: s.score, reverse=True)

    # Bucketization (adjust thresholds to taste)
    #   Tier 1: strong positive score
    #   Tier 2: mild positive score
    #   Watch : top names even if score small/negative, limited count
    for i, s in enumerate(snapshots):
        idea = {
            "symbol": s.symbol,
            "last": round(s.last, 2),
            "rsi": None if not math.isfinite(s.rsi) else round(s.rsi, 1),
            "m1h_pct": None if not math.isfinite(s.m1h) else round(100 * s.m1h, 2),
            "vol20": None if not math.isfinite(s.vol20) else round(s.vol20, 4),
            "score": round(s.score, 2),
        }

        if s.score >= 2.0:
            tier1.append(idea)
        elif s.score >= 0.5:
            tier2.append(idea)
        elif i < 10:  # keep a small watchlist
            watch.append(idea)

    return {"tier1": tier1, "tier2": tier2, "watch": watch, "logs": logs}