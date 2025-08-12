from __future__ import annotations
import os
from datetime import datetime
from typing import Any, Dict, List, Tuple

from core.signals import generate_ranked_ideas   # existing module in your repo
from core.emailer import send_email              # existing mailer

# --------- runtime config ---------
TICKERS = [
    "SPY", "AAPL", "TSLA", "MSFT", "AMZN",
    "GOOGL", "NVDA", "META", "NFLX", "AMD",
    "AAL", "PLTR", "F", "RIVN", "SOFI",
]

HORIZON_HOURS = int(os.getenv("HORIZON_HOURS", 2))
BIAS_MODE = os.getenv("BIAS_MODE", "revert")  # or "trend"

# Looser gating settings (match filter_options defaults)
FILTER_CFG: Dict[str, Any] = {
    "MIN_OI": 50,
    "MAX_SPREAD_PCT": 0.50,
    "DTE_MIN": 0,
    "DTE_MAX": 21,
    "MIN_PRICE": 0.10,
    "MAX_PRICE": 10.00,
    "STRIKES_RANGE": 8,
    "MIN_SCORE_TIER1": 72,
    "MIN_SCORE_TIER2": 60,
    "WATCHLIST_TOP": 5,
}

HEADER_LINE = (
    f"horizon={HORIZON_HOURS}h, "
    f"DTE[{FILTER_CFG['DTE_MIN']}–{FILTER_CFG['DTE_MAX']}], "
    f"${FILTER_CFG['MIN_PRICE']:.2f}-${FILTER_CFG['MAX_PRICE']:.2f}, "
    f"minOI={FILTER_CFG['MIN_OI']}, "
    f"maxSpread={int(FILTER_CFG['MAX_SPREAD_PCT']*100)}%"
)

# --------- helpers ---------
def _format_rows(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "None"
    lines = []
    for r in rows:
        sym = r.get("symbol") or r.get("contractSymbol") or r.get("ticker", "?")
        dirn = r.get("side") or r.get("direction", "?")
        strike = r.get("strike", "?")
        dte = r.get("dte", r.get("daysToExpiration", "?"))
        mid = r.get("mid", r.get("price", r.get("lastPrice", "?")))
        score = r.get("score", "?")
        reason = r.get("reason", "")
        lines.append(f"- {sym} {dirn} @ {strike} (DTE {dte}, mid {mid}, score {score}) {reason}")
    return "\n".join(lines)

def _build_email(
    tier1: List[Dict[str, Any]],
    tier2: List[Dict[str, Any]],
    watch: List[Dict[str, Any]],
    debug_lines: List[str]
) -> Tuple[str, str]:
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    subject = "Option Pro – Ranked Ideas"

    body = []
    body.append("Option Pro – Ranked Ideas")
    body.append("")
    body.append(ts)
    body.append("")
    body.append(HEADER_LINE)
    body.append("")
    body.append("Tier 1 (High Conviction)")
    body.append(_format_rows(tier1))
    body.append("")
    body.append("Tier 2 (Moderate)")
    body.append(_format_rows(tier2))
    body.append("")
    body.append("Watchlist (Top Fallback)")
    body.append(_format_rows(watch))
    body.append("")
    if debug_lines:
        body.append("Debug")
        body.extend(debug_lines)

    return subject, "\n".join(body)

# --------- main run ---------
def run():
    print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] 🚀 Running Option Pro (ranked)…")

    # generate_ranked_ideas return type can be tuple or dict depending on your current signals.py.
    result = generate_ranked_ideas(
        tickers=TICKERS,
        horizon_hours=HORIZON_HOURS,
        bias_mode=BIAS_MODE,
        filter_cfg=FILTER_CFG,
    )

    # Resilient unpack
    tier1: List[Dict[str, Any]] = []
    tier2: List[Dict[str, Any]] = []
    watch: List[Dict[str, Any]] = []
    debug_lines: List[str] = []

    if isinstance(result, dict):
        tier1 = result.get("tier1", []) or result.get("t1", []) or []
        tier2 = result.get("tier2", []) or result.get("t2", []) or []
        watch = result.get("watchlist", []) or result.get("watch", []) or []
        dbg = result.get("debug", []) or result.get("notes", [])
        if isinstance(dbg, list):
            debug_lines = [str(x) for x in dbg]
    elif isinstance(result, (list, tuple)) and len(result) >= 3:
        tier1, tier2, watch = result[0], result[1], result[2]
        if len(result) >= 4 and isinstance(result[3], dict):
            stats = result[3]
            debug_lines = [f"{k}: {v}" for k, v in stats.items()]
    else:
        debug_lines = ["⚠ Unexpected result structure from generate_ranked_ideas"]

    subject, body = _build_email(tier1, tier2, watch, debug_lines)

    # Send mail
    send_email(
        subject=subject,
        body=body,
        to_addr=os.getenv("TO_EMAIL"),
        from_addr=os.getenv("EMAIL_ADDRESS"),
        password=os.getenv("EMAIL_PASSWORD"),
    )

    print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] ✅ Email sent.")

if __name__ == "__main__":
    run()