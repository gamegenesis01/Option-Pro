# main.py
import os
from datetime import datetime

from core.signals import generate_ranked_ideas
from notifications.email_alerts import send_email_alert


# ----------------------- Configuration -----------------------
# NOTE: generate_ranked_ideas expects ONE dict `config`.
# Keep keys minimal and stable.
DEFAULT_TICKERS = [
    "SPY", "AAPL", "TSLA", "MSFT", "AMZN",
    "GOOGL", "NVDA", "META", "NFLX", "AMD",
    "AAL", "PLTR", "F", "RIVN", "SOFI",
]

CONFIG = {
    "tickers": DEFAULT_TICKERS,
    # Forecast horizon in hours (used by features/forecast)
    "horizon_hours": int(os.getenv("HORIZON_HOURS", "2")),
    # How many IV points to “shock” when estimating price change
    "exp_div_pts": float(os.getenv("EXP_DIV_PTS", "0.5")),
    # Liquidity / quality filters used inside core.filter_options
    "filter_cfg": {
        "min_open_interest": int(os.getenv("MIN_OI", "100")),
        "max_spread_pct": float(os.getenv("MAX_SPREAD_PCT", "35")),
        # Optional: nearest strikes around spot to consider
        "strike_window_dollars": float(os.getenv("STRIKE_WIN_DOLLARS", "8")),
        # Optional: maximum DTE to consider (0‑14 means 0..14 inclusive)
        "max_dte_days": int(os.getenv("MAX_DTE_DAYS", "14")),
    },
    # Optional caps for email sections (None = unlimited)
    "max_tier1": int(os.getenv("MAX_TIER1", "10")),
    "max_tier2": int(os.getenv("MAX_TIER2", "10")),
    "max_watch": int(os.getenv("MAX_WATCH", "10")),
}


# ----------------------- App entrypoint -----------------------
def run() -> None:
    stamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{stamp}] 🚀 Running Option Pro (ranked)…")

    try:
        ranked = generate_ranked_ideas(CONFIG)
    except Exception as e:
        # Fail soft: send whatever we have as text for debugging
        print(f"❌ generate_ranked_ideas failed: {e!r}")
        send_email_alert({"logs": [f"generate_ranked_ideas failed: {e!r}"]})
        raise

    # `ranked` should be a dict with: tier1, tier2, watch, all, logs (any may be empty)
    # The email helper gracefully handles missing/empty sections.
    try:
        send_email_alert(ranked)  # subject default is fine
    except Exception as e:
        print(f"❌ Email send failed: {e!r}")

    # Console summary
    t1 = len(ranked.get("tier1", [])) if isinstance(ranked, dict) else 0
    t2 = len(ranked.get("tier2", [])) if isinstance(ranked, dict) else 0
    wt = len(ranked.get("watch", [])) if isinstance(ranked, dict) else 0
    print(f"✅ Done. Tier1={t1}, Tier2={t2}, Watch={wt}")


if __name__ == "__main__":
    run()