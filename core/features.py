# core/features.py
from __future__ import annotations

import numpy as np
import pandas as pd

from .fetch_data import latest_close_series

def latest_snapshot(px: pd.DataFrame | pd.Series) -> dict:
    """
    Build a robust snapshot from price history:
    - extracts a 1-D close series
    - removes NaN/Inf and non-positive values
    - returns last valid close and a simple 1-day return
    """
    s = latest_close_series(px)

    # Clean it: coerce finiteness and positivity
    s = pd.to_numeric(s, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    s = s[s > 0]

    if s.empty or not np.isfinite(s.iloc[-1]):
        return {"ok": False, "reason": "bad_snapshot", "last_close": np.nan}

    last = float(s.iloc[-1])
    prev = float(s.iloc[-2]) if len(s) > 1 and np.isfinite(s.iloc[-2]) else last
    ret_1d = (last / prev - 1.0) if prev > 0 else 0.0

    return {
        "ok": True,
        "last_close": last,
        "ret_1d": ret_1d,
        "series": s,  # cleaned, UTC-sorted Series
    }


def add_features(px: pd.DataFrame | pd.Series) -> dict:
    """
    Example lightweight features on the cleaned close series:
    - 5/20 MA slope signal
    - simple z-score vs 20d mean/std
    """
    snap = latest_snapshot(px)
    if not snap["ok"]:
        return {"ok": False, "reason": snap["reason"]}

    s: pd.Series = snap["series"]  # type: ignore

    # Moving averages
    ma5 = s.rolling(5).mean()
    ma20 = s.rolling(20).mean()

    # z-score
    mu20 = s.rolling(20).mean()
    sd20 = s.rolling(20).std(ddof=0)
    z20 = (s - mu20) / sd20.replace(0, np.nan)

    feats = {
        "ok": True,
        "last_close": snap["last_close"],
        "ret_1d": snap["ret_1d"],
        "ma5": float(ma5.iloc[-1]) if len(ma5) else np.nan,
        "ma20": float(ma20.iloc[-1]) if len(ma20) else np.nan,
        "z20": float(z20.iloc[-1]) if len(z20) else np.nan,
    }
    return feats