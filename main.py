# main.py
import os
from datetime import datetime
from typing import Any, Dict, List

from core.signals import generate_ranked_ideas
from core.emailer import send_email  # signature: send_email(subject, body, to_email)


# -----------------------------
# Config (env-overridable)
# -----------------------------
# Comma-separated list in env; otherwise fall back to a default basket
DEFAULT_TICKERS = [
    "SPY", "AAPL", "TSLA", "MSFT", "AMZN",
    "GOOGL", "NVDA", "META", "NFLX", "AMD",
    "AAL", "PLTR", "F", "RIVN", "SOFI"
]

_env_tickers = os.getenv("TICKERS", "")
TICKERS: List[str] = (
    [t.strip().upper() for t in _env_tickers.split(",") if t.strip()]
    if _env_tickers.strip()
    else DEFAULT_TICKERS
)

# Hours ahead for the P&L/Greeks projection
HORIZON_HOURS: int = int(os.getenv("HORIZON_HOURS", "2"))

# Expected change in IV (points, not %) over the horizon (e.g., 0.5 ⇒ +0.5 IV pts)
EXP_DIV_PTS: float = float(os.getenv("EXP_DIV_PTS", "0.5"))

# Email
TO_EMAIL = os.getenv("TO_EMAIL", "").strip()

# Option filtering knobs (can be expanded as needed)
FILTER_CFG: Dict[str, Any] = {
    "min_open_interest": int(os.getenv("MIN_OI", "100")),
    "max_spread_pct": float(os.getenv("MAX_SPREAD_PCT", "35")),  # % of mid
    "dte_min": int(os.getenv("DTE_MIN", "0")),                   # same week okay
    "dte_max": int(os.getenv("DTE_MAX", "14")),                  # next 2 weeks
    "near_money_usd": float(os.getenv("NEAR_MONEY_USD", "8")),   # within $±
}


# -----------------------------
# Helpers
# -----------------------------
def _fmt_line(item: Dict[str, Any]) -> str:
    """Format a single contract dict into a compact one-liner."""
    sym = item.get("symbol", "?")
    typ = item.get("type", "?")
    k   = item.get("strike", "?")
    exp = item.get("expiry", "?")
    mid = item.get("mid", "?")
    exp_roi = item.get("exp_roi", None)
    roi_txt = f"{exp_roi:.2f}%" if isinstance(exp_roi, (int, float)) else "n/a"
    return f"- [{sym}] {typ.upper()} {k} exp {exp} @ ~{mid}  → ROI {roi_txt}"


def build_email(result: Dict[str, Any]) -> str:
    """Turn the result dict from generate_ranked_ideas into an email body."""
    ts = datetime.utcnow().isoformat(sep=" ", timespec="seconds")

    tier1 = result.get("tier1", []) or []
    tier2 = result.get("tier2", []) or []
    watch = result.get("watch", []) or []
    allc  = result.get("all", []) or []
    logs  = result.get("logs", []) or []

    # Header with current runtime configuration
    header = (
        f"{ts}\n\n"
        f"horizon={HORIZON_HOURS}h, dσ={EXP_DIV_PTS}pt(s), "
        f"DTE[{FILTER_CFG['dte_min']}-{FILTER_CFG['dte_max']}], "
        f"±${FILTER_CFG['near_money_usd']}, "
        f"minOI={FILTER_CFG['min_open_interest']}, "
        f"maxSpread={int(FILTER_CFG['max_spread_pct'])}%\n"
    )

    def section(title: str, items: List[Dict[str, Any]]) -> str:
        if not items:
            return f"\n{title}\nNone\n"
        lines = "\n".join(_fmt_line(it) for it in items)
        return f"\n{title}\n{lines}\n"

    body = [
        header,
        section("Tier 1 (High Conviction)", tier1),
        section("Tier 2 (Moderate)",       tier2),
        section("Watchlist (Top Fallback)", watch),
        section("All candidates (debug)",   allc[:30]),  # limit length
        "\nDebug\n" + "\n".join(str(l) for l in logs)
    ]
    return "".join(body)


# -----------------------------
# Orchestration
# -----------------------------
def run() -> None:
    print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] 🚀 Running Option Pro (ranked)…")

    # IMPORTANT: generate_ranked_ideas expects ONE argument (a config dict)
    config: Dict[str, Any] = {
        "tickers": TICKERS,
        "horizon_hours": HORIZON_HOURS,
        "exp_div_pts": EXP_DIV_PTS,
        "filters": FILTER_CFG,
    }

    # Call core logic
    result = generate_ranked_ideas(config)

    # Build and send email (or print if TO_EMAIL not set)
    subject = "Option Pro – Ranked Ideas"
    body = build_email(result)

    if TO_EMAIL:
        try:
            send_email(subject, body, TO_EMAIL)
            print("📧 Email sent.")
        except Exception as e:
            print(f"⚠ Email send failed: {e}\n\nFalling back to stdout below:\n")
            print("\n" + "=" * 60 + "\n" + body + "\n" + "=" * 60 + "\n")
    else:
        print("\n" + "=" * 60 + "\n" + body + "\n" + "=" * 60 + "\n")


if __name__ == "__main__":
    run()