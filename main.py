# main.py
from __future__ import annotations

import os
from datetime import datetime

from core.signals import generate_ranked_ideas
from core.emailer import send_email


def fmt_contract(c: dict) -> str:
    # one-line summary for email
    base = (
        f"{c['symbol']} {c['expiry']} {c['type'].upper()} "
        f"{c['strike']} | mid {c.get('mid', 'NA')} | "
        f"Δ {c.get('delta','NA')} Γ {c.get('gamma','NA')} Θd {c.get('theta_day','NA')} "
        f"V {c.get('vega','NA')} | expΔ ${c.get('exp_change','NA')} | ROI {c.get('exp_roi','NA'):.2f}%"
        if 'exp_roi' in c else
        f"{c['symbol']} {c['expiry']} {c['type'].upper()} {c['strike']} | mid {c.get('mid','NA')}"
    )
    return base


def build_email(result: dict) -> str:
    meta = result.get("meta", {})
    horizon_min = meta.get("horizon_min")
    header = [
        "Option Pro – Ranked Ideas",
        "",
        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "",
        f"horizon={horizon_min}m, "
        f"DTE<= {meta.get('max_dte_days', 'NA')}, "
        f"strikes_around={meta.get('strikes_around', 'NA')}",
        ""
    ]

    def block(title: str, rows: list[dict]) -> list[str]:
        if not rows:
            return [f"{title}:", "None", ""]
        out = [f"{title}:"]
        for r in rows:
            out.append("- " + fmt_contract(r))
        out.append("")
        return out

    body_lines = []
    body_lines += block("Tier 1 (High Conviction)", result.get("tier1", []))
    body_lines += block("Tier 2 (Moderate)", result.get("tier2", []))
    body_lines += block("Watchlist (Top Fallback)", result.get("watch", []))

    # compact “all” dump (useful when tiers empty)
    if result.get("all"):
        body_lines += ["All candidates (debug):"]
        for r in result["all"][:50]:
            body_lines.append("- " + fmt_contract(r))
        body_lines.append("")

    # logs
    logs = result.get("logs", [])
    if logs:
        body_lines += ["Debug", ""]
        for line in logs:
            body_lines.append(line)

    return "\n".join(header + body_lines)


def run():
    # =========================
    # CONFIG (edit as you like)
    # =========================
    universe = [
        "SPY", "AAPL", "TSLA", "MSFT", "AMZN",
        "GOOGL", "NVDA", "META", "NFLX",
        "AMD", "AAL", "PLTR", "F", "RIVN", "SOFI"
    ]

    horizon_min = 120            # <— 2 hours
    max_dte_days = 14
    strikes_around = 6

    # Filters – keep modest to avoid “no trades”
    filter_cfg = {
        "min_oi": 50,
        "max_spread_pct": 0.40,   # 40% of mid
        "min_mid": 0.15,          # skip dust
    }

    # Scoring – weights for ranker
    score_cfg = {
        "w_delta": 0.35,
        "w_gamma": 0.10,
        "w_theta": 0.10,
        "w_vega": 0.15,
        "w_exp_roi": 0.30,
        "penalty_wide_spread": 0.10,
    }

    print(f"[{datetime.utcnow()}] 🚀 Running Option Pro (ranked)…")
    result = generate_ranked_ideas(
        universe,
        horizon_min=horizon_min,      # <— new unified param
        max_dte_days=max_dte_days,
        strikes_around=strikes_around,
        filter_cfg=filter_cfg,
        score_cfg=score_cfg,
    )

    subject = "Option Pro – Ranked Ideas"
    body = build_email(result)

    to_email = os.getenv("TO_EMAIL")
    if to_email:
        send_email(
            subject=subject,
            body=body,
            to_email=to_email,
        )
        print("📧 Email sent.")
    else:
        # fallback to console (e.g., local runs)
        print("\n" + "=" * 60 + "\n" + body + "\n" + "=" * 60 + "\n")


if __name__ == "__main__":
    run()