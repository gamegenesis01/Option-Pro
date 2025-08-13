# core/signals.py
from __future__ import annotations

import math
import traceback
from typing import Dict, List, Tuple, Any

import pandas as pd

# ---- Internal imports (must exist in your repo) -----------------------------
from .fetch_data import get_price_history                   # returns a pd.DataFrame or None
from .features import add_features, latest_snapshot         # your existing feature builders
from .filter_options import filter_contracts                # you said you already have this
from .scoring import score_contracts                        # ranks contracts

# ----------------------------------------------------------------------------
# Yahoo-style limits (practical caps to avoid empty returns)
# ----------------------------------------------------------------------------
_INTRA_LIMITS = {
    "1m": 7,          # days
    "2m": 60,
    "5m": 60,
    "15m": 60,
    "30m": 60,
    "60m": 730,       # ~2y, but many providers clamp tighter; we keep generous
    "90m": 730,
    "1h": 730,
}
_DAILY_INTERVALS = {"1d", "1wk", "1mo"}

# Reason labels used in logs
REASON_BAD_SNAPSHOT = "bad_snapshot"
REASON_NO_DATA = "no_data"
REASON_UNEXPECTED = "unexpected"


def _cap_lookback_for_interval(interval: str, lookback_days: int) -> int:
    """Clamp lookback to something the data source will actually serve."""
    if interval in _INTRA_LIMITS:
        limit = _INTRA_LIMITS[interval]
        return min(lookback_days, limit)
    return lookback_days


def _is_intraday(interval: str) -> bool:
    return interval not in _DAILY_INTERVALS


def _safe_fetch_prices(
    symbol: str,
    lookback_days: int,
    interval: str,
    *,
    enable_fallback: bool = True,
) -> Tuple[pd.DataFrame | None, List[str]]:
    """
    Tries to fetch OHLCV with sensible clamping + fallbacks.
    Returns (df_or_none, debug_lines)
    """
    dbg: List[str] = []
    try:
        lb = _cap_lookback_for_interval(interval, lookback_days)
        if lb != lookback_days:
            dbg.append(f"[{symbol}] capped lookback_days {lookback_days} -> {lb} for interval {interval}")
        df = get_price_history(symbol, lookback_days=lb, interval=interval)

        if df is not None and not df.empty:
            dbg.append(f"[{symbol}] primary fetch OK: rows={len(df)}, interval={interval}, lb_days={lb}")
            return df, dbg

        dbg.append(f"[{symbol}] primary fetch EMPTY: interval={interval}, lb_days={lb}")

        if not enable_fallback:
            return None, dbg

        # ---- Fallbacks -----------------------------------------------------
        # 1) If intraday request failed, try a coarser intraday interval
        if _is_intraday(interval):
            fallback_chain = []
            # Prefer stepping up to 15m, then 30m, then 60m for stability
            if interval != "15m":
                fallback_chain.append(("15m", _cap_lookback_for_interval("15m", lookback_days)))
            if interval != "30m":
                fallback_chain.append(("30m", _cap_lookback_for_interval("30m", lookback_days)))
            if interval not in ("60m", "1h"):
                fallback_chain.append(("60m", _cap_lookback_for_interval("60m", lookback_days)))

            for iv, lb2 in fallback_chain:
                try:
                    df2 = get_price_history(symbol, lookback_days=lb2, interval=iv)
                    if df2 is not None and not df2.empty:
                        dbg.append(f"[{symbol}] intraday fallback OK: rows={len(df2)}, interval={iv}, lb_days={lb2}")
                        return df2, dbg
                    dbg.append(f"[{symbol}] intraday fallback EMPTY: interval={iv}, lb_days={lb2}")
                except Exception as e:
                    dbg.append(f"[{symbol}] intraday fallback ERR {iv}: {e}")

        # 2) Final fallback to daily bars so we always get *something*
        daily_lb = max(lookback_days, 30)
        try:
            df3 = get_price_history(symbol, lookback_days=daily_lb, interval="1d")
            if df3 is not None and not df3.empty:
                dbg.append(f"[{symbol}] daily fallback OK: rows={len(df3)}, interval=1d, lb_days={daily_lb}")
                return df3, dbg
            dbg.append(f"[{symbol}] daily fallback EMPTY: interval=1d, lb_days={daily_lb}")
        except Exception as e:
            dbg.append(f"[{symbol}] daily fallback ERR 1d: {e}")

        return None, dbg

    except Exception as e:
        dbg.append(f"[{symbol}] _safe_fetch_prices() UNEXPECTED: {e}")
        return None, dbg


def _make_issue(symbol: str, reason: str, detail: str) -> str:
    """Uniform log line."""
    return f"- [{symbol}] ⚠ Forecast issue: {reason} ({detail})"


def _clean_recent_nans(df: pd.DataFrame, symbol: str, dbg: List[str]) -> pd.DataFrame:
    """Drop leading/trailing all-NaN rows; keep middle NaNs for features to handle."""
    before = len(df)
    # Keep index; drop rows where close is NaN
    if "close" in df.columns:
        df2 = df.dropna(subset=["close"])
    else:
        # Try standard Yahoo column capitalization if features expect 'Close'
        col = "Close" if "Close" in df.columns else None
        if col:
            df2 = df.dropna(subset=[col])
        else:
            dbg.append(f"[{symbol}] no close column found; columns={list(df.columns)}")
            return df
    after = len(df2)
    if after != before:
        dbg.append(f"[{symbol}] dropped NaN-close rows: {before} -> {after}")
    return df2


def generate_ranked_ideas(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main entry point used by main.py

    Expected keys in config (defaults applied if missing):
      - tickers: List[str]
      - lookback_days: int (default 30)
      - px_interval: str (default "15m")
      - horizon_hours: int (default 2)                # used by your features/scoring
      - filter_cfg: Dict[str, Any] (default {})
      - top_n_watch: int (default 20)                 # for watchlist fallback

    Returns:
      {
        "tier1": List[Dict],          # high conviction
        "tier2": List[Dict],          # moderate
        "watch": List[Dict],          # top fallback if tiers are empty
        "all":   List[Dict],          # all scored contracts (filtered)
        "logs":  List[str],           # human-readable logs for email
      }
    """
    tickers: List[str] = config.get("tickers", [])
    lookback_days: int = int(config.get("lookback_days", 30))
    px_interval: str = str(config.get("px_interval", "15m"))
    horizon_hours: int = int(config.get("horizon_hours", 2))
    filter_cfg: Dict[str, Any] = config.get("filter_cfg", {}) or {}
    top_n_watch: int = int(config.get("top_n_watch", 20))

    logs: List[str] = []
    all_scored: List[Dict[str, Any]] = []
    per_symbol_debug: List[str] = []

    for symbol in tickers:
        try:
            # -------- robust fetch with fallback + detailed debug -------------
            px, dbg = _safe_fetch_prices(symbol, lookback_days, px_interval, enable_fallback=True)
            per_symbol_debug.extend(dbg)

            if px is None or px.empty:
                logs.append(_make_issue(symbol, REASON_NO_DATA, "empty after fallbacks"))
                continue

            # standardize columns for features if needed
            # Expecting columns roughly like: index=Datetime, ['open','high','low','close','volume']
            # If provider returns 'Open' etc, lower them:
            lowered = {c: c.lower() for c in px.columns}
            if any(c.isupper() for c in px.columns):
                px = px.rename(columns=lowered)

            # basic cleanliness
            px = _clean_recent_nans(px, symbol, per_symbol_debug)
            if px is None or px.empty:
                logs.append(_make_issue(symbol, REASON_BAD_SNAPSHOT, "no rows after cleaning"))
                continue

            # quick sanity: last close should be finite
            last_close = px["close"].iloc[-1] if "close" in px.columns else float("nan")
            if last_close is None or isinstance(last_close, float) and (math.isnan(last_close) or math.isinf(last_close)):
                logs.append(_make_issue(symbol, REASON_BAD_SNAPSHOT, "invalid last close"))
                continue

            # -------- features & snapshot ------------------------------------
            # Your add_features should add exp_dS/exp_dIV etc based on horizon_hours
            px_feat = add_features(px, horizon_hours=horizon_hours)
            snap = latest_snapshot(px_feat)
            if snap is None:
                logs.append(_make_issue(symbol, REASON_BAD_SNAPSHOT, "latest_snapshot returned None"))
                continue

            # -------- get/score options --------------------------------------
            # filter_contracts must create a universe for this symbol using 'snap'
            contracts = filter_contracts(symbol, snap, cfg=filter_cfg)
            if not contracts:
                logs.append(_make_issue(symbol, "no_contracts", "no liquid contracts after filter"))
                continue

            scored = score_contracts(contracts, horizon_hours=horizon_hours)
            for row in scored:
                row["_symbol"] = symbol  # tag for later grouping
            all_scored.extend(scored)

        except Exception as e:
            logs.append(_make_issue(symbol, REASON_UNEXPECTED, str(e)))
            per_symbol_debug.append(f"[{symbol}] Traceback:\n{traceback.format_exc()}")

    # ----------------- rank + tiers ------------------------------------------
    if not all_scored:
        # Include useful fetch/debug lines when nothing scored
        logs.extend(per_symbol_debug)
        return {
            "tier1": [],
            "tier2": [],
            "watch": [],
            "all": [],
            "logs": logs,
        }

    # Sort by our score (descending). Your score_contracts must add 'score'.
    all_scored.sort(key=lambda r: r.get("score", 0.0), reverse=True)

    # Split tiers by simple thresholds; you can tune these
    tier1 = [r for r in all_scored if r.get("score", 0.0) >= 0.80]
    tier2 = [r for r in all_scored if 0.60 <= r.get("score", 0.0) < 0.80]

    # Fallback watchlist: top N overall if no confirmed tiers
    watch = []
    if not tier1 and not tier2:
        watch = all_scored[:top_n_watch]

    # Trim debug; but include fetch lines to help diagnose in email
    logs.extend(per_symbol_debug)

    return {
        "tier1": tier1,
        "tier2": tier2,
        "watch": watch,
        "all": all_scored,
        "logs": logs,
    }