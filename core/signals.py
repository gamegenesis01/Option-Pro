# core/signals.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from .fetch_data import get_price_history, latest_close_series


# ---------- helpers: indicators kept 1‑D safe ----------

def _rsi(close: pd.Series, window: int = 14) -> float:
    """Return latest RSI value (float). Works on a *1‑D* close Series."""
    s = latest_close_series(close)
    if s.size < window + 1:
        raise ValueError("too_few_points_for_rsi")
    delta = s.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    roll_up = gain.rolling(window).mean()
    roll_down = loss.rolling(window).mean()
    rs = roll_up / (roll_down.replace(0, np.nan))
    rsi = 100.0 - (100.0 / (1.0 + rs))
    val = float(rsi.iloc[-1])
    if np.isnan(val) or np.isinf(val):
        raise ValueError("nan_rsi")
    return val


def _zscore(close: pd.Series, window: int = 20) -> float:
    s = latest_close_series(close)
    if s.size < window:
        raise ValueError("too_few_points_for_z")
    roll = s.rolling(window)
    mu = roll.mean().iloc[-1]
    sd = roll.std(ddof=0).iloc[-1]
    if sd == 0 or np.isnan(sd) or np.isnan(mu):
        raise ValueError("bad_std")
    return float((s.iloc[-1] - mu) / sd)


def _mom(close: pd.Series, lookback: int = 5) -> float:
    s = latest_close_series(close)
    if s.size <= lookback:
        raise ValueError("too_few_points_for_mom")
    return float((s.iloc[-1] / s.iloc[-lookback]) - 1.0)


@dataclass
class Idea:
    symbol: str
    rsi: float
    z: float
    mom: float
    score: float


def _score(rsi: float, z: float, mom: float) -> float:
    """
    Simple combined score:
    - Prefer mid‑range RSI (not overbought/oversold).
    - Favor positive momentum.
    - Favor mild positive z‑score (above mean, not extreme).
    """
    # Normalize components to roughly comparable scales
    rsi_penalty = -abs(rsi - 50.0) / 50.0  # 0 at 50; -1 at extremes
    mom_term = mom  # momentum as %
    z_term = -abs(z)  # prefer near 0 (mean‑reversion bias)
    return 1.5 * rsi_penalty + 1.0 * mom_term + 0.2 * z_term


# ---------- public API ----------

def generate_ranked_ideas(config: Dict) -> Dict[str, List[Dict] | List[str]]:
    """
    Core entry point used by main.py.

    Parameters (expected keys in `config`):
      - tickers: List[str]
      - lookback_days: int
      - px_interval: str (e.g., "1d", "1h")
      - max_watchlist: int (fallback list size when no tiers)
    Returns a dict with keys: 'tier1', 'tier2', 'watch', 'logs'
    Each tier/watch entry is a dict describing the idea.
    """
    tickers: List[str] = config.get("tickers", [])
    lookback_days: int = int(config.get("lookback_days", 60))
    px_interval: str = str(config.get("px_interval", "1d"))
    max_watch: int = int(config.get("max_watchlist", 10))

    tier1: List[Dict] = []
    tier2: List[Dict] = []
    watch: List[Dict] = []
    logs: List[str] = []

    candidates: List[Idea] = []

    for symbol in tickers:
        try:
            px = get_price_history(symbol, lookback_days=lookback_days, interval=px_interval)
            closes = latest_close_series(px)

            # Basic sanity checks
            if closes.empty or closes.size < 25 or not np.isfinite(closes.iloc[-1]):
                logs.append(f"- [{symbol}] ⚠ Forecast issue: bad_snapshot (invalid last close)")
                continue

            rsi = _rsi(closes, window=14)
            z = _zscore(closes, window=20)
            mom = _mom(closes, lookback=5)
            score = _score(rsi, z, mom)

            candidates.append(Idea(symbol=symbol, rsi=rsi, z=z, mom=mom, score=score))

        except Exception as e:
            # Keep the engine resilient — log and move on.
            logs.append(f"- [{symbol}] ⚠ Unexpected: {type(e).__name__}: {e!s}")

    # Rank by score (desc)
    if candidates:
        ranked = sorted(candidates, key=lambda x: x.score, reverse=True)

        # Simple bucketing: top 2 -> tier1, next 3 -> tier2, rest -> watch (up to max_watch)
        for i, idea in enumerate(ranked):
            entry = {
                "symbol": idea.symbol,
                "rsi": round(idea.rsi, 2),
                "zscore": round(idea.z, 2),
                "momentum_5": round(idea.mom * 100, 2),  # %
                "score": round(idea.score, 4),
            }
            if i < 2:
                tier1.append(entry)
            elif i < 5:
                tier2.append(entry)
            elif len(watch) < max_watch:
                watch.append(entry)

    # If nothing made it (e.g., bad data day), keep watch empty or filled with top symbols we tried
    return {
        "tier1": tier1,
        "tier2": tier2,
        "watch": watch,
        "logs": logs,
    }