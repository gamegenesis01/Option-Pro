# main.py
import os
from datetime import datetime
from typing import Dict, List, Any

from core.signals import generate_ranked_ideas
from core.emailer import send_email


# =========================
# Config (edit as you like)
# =========================
TICKERS: List[str] = [
    "SPY", "AAPL", "TSLA", "MSFT", "AMZN", "GOOGL",
    "NVDA", "META", "NFLX", "AMD", "AAL", "PLTR",
    "F", "RIVN", "SOFI"
]

# 1–3 hour horizon is what we designed for options
HORIZON_HOURS: int = int(os.getenv("HORIZON_HOURS", "2"))

# Expected IV change in points (e.g., 0.5 => +0.50 IV pts)
EXP_DIV_PTS: float = float(os.getenv("EXP_DIV_PTS", "0.5"))

# Filters for options (liquidity/quality). Edit as desired.
FILTER_CFG = {
    "min_open_interest": int(os.getenv("MIN_OI", "100")),
    "max_bid_ask_spread": float(os.getenv("MAX_SPREAD", "0.35")),  # 35% of mid
    "dte_min": int(os.getenv("DTE_MIN", "0")),    # 0 => same-week allowed
    "dte_max": int(os.getenv("DTE_MAX", "14")),   # within 2 weeks
    "price_band_abs": float(os.getenv("PRICE_BAND_ABS", "8.0")),  # +/-$ around spot
    "allow_zero_iv": os.getenv("ALLOW_ZERO_IV", "false").lower() in ("1","true","yes"),
}

# Email destination (set in GitHub Secrets)
TO_EMAIL = os.getenv("TO_EMAIL", "").strip()


# =========================
# Helpers
# =========================
def _fmt_num(x: Any) -> str:
    if x is None:
        return "None"
    try:
        # show integers nicely, decimals with up to 2 places
        if isinstance(x, (int,)):
            return str(x)
        f = float(x)
        if abs(f) >= 1000:
            return f"{f:,.0f}"
        elif abs(f) >= 100:
            return f"{f:,.1f}"
        else:
            return f"{f:.2f}"
    except Exception:
        return str(x)


def _format_contract_line(c: Dict[str, Any]) -> str:
    """
    Render a single contract dict to a compact one-line string.
    Expected keys (as produced by our pipeline):
      symbol, expiry, type, strike, mid, bid, ask, iv, delta, gamma, theta_day,
      vega, rho, exp_dS, exp_dIV_pts, horizon_h, exp_change, exp_roi
    """
    parts = [
        f"{c.get('symbol', '?')}",
        c.get("expiry", "?"),
        c.get("type", "?"),
        f"K={_fmt_num(c.get('strike'))}",
        f"mid={_fmt_num(c.get('mid'))}",
        f"iv={_fmt_num(c.get('iv'))}%",
        f"Δ={_fmt_num(c.get('delta'))}",
        f"Γ={_fmt_num(c.get('gamma'))}",
        f"Θ/d={_fmt_num(c.get('theta_day'))}",
        f"V={_fmt_num(c.get('vega'))}",
        f"ρ={_fmt_num(c.get('rho'))}",
        f"expΔS={_fmt_num(c.get('exp_dS'))}",
        f"Δσ={_fmt_num(c.get('exp_dIV_pts'))}pt",
        f"H={_fmt_num(c.get('horizon_h'))}h",
        f"expΔ={_fmt_num(c.get('exp_change'))}",
        f"ROI={_fmt_num(c.get('exp_roi'))}%",
    ]
    return "- " + " | ".join(parts)


def _format_list(block_title: str, items: List[Dict[str, Any]]) -> str:
    if not items:
        return f"{block_title}:\nNone\n"
    lines = [f"{block_title}:"]
    for c in items:
        lines.append(_format_contract_line(c))
    lines.append("")  # blank line after block
    return "\n".join(lines)


def _format_logs(logs: List[str]) -> str:
    if not logs:
        return "logs:\nNone\n"
    out = ["logs:"]
    out.extend(f"- {msg}" for msg in logs)
    out.append("")
    return "\n".join(out)


def build_email(result: Dict[str, Any]) -> str:
    """
    result is expected to be a dict with keys:
      tier1, tier2, watch, all, logs
    Each of tier1/tier2/watch/all is a list of contract dicts.
    logs is a list of strings.
    """
    ts = datetime.utcnow().isoformat(sep=" ", timespec="seconds")
    header = [
        "Option Pro – Ranked Ideas",
        ts,
        "",
        f"horizon={HORIZON_HOURS}h, dσ={_fmt_num(EXP_DIV_PTS)}pt(s), "
        f"DTE[{FILTER_CFG['dte_min']}-{FILTER_CFG['dte_max']}], "
        f"±${_fmt_num(FILTER_CFG['price_band_abs'])}, "
        f"minOI={FILTER_CFG['min_open_interest']}, "
        f"maxSpread={int(FILTER_CFG['max_bid_ask_spread'] * 100)}%",
        ""
    ]

    body_blocks = [
        _format_list("tier1", result.get("tier1", [])),
        _format_list("tier2", result.get("tier2", [])),
        _format_list("watch", result.get("watch", [])),
        _format_list("all", result.get("all", [])),
        _format_logs(result.get("logs", [])),
    ]

    return "\n".join(header + body_blocks)


# =========================
# Main
# =========================
def run() -> None:
    print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] 🚀 Running Option Pro (ranked)…")

    # Generate ideas (positional args to avoid signature mismatch)
    result = generate_ranked_ideas(TICKERS, HORIZON_HOURS, EXP_DIV_PTS, FILTER_CFG)

    # Build email/text
    subject = "Option Pro – Ranked Ideas"
    message = build_email(result)

    # Send or print
    if TO_EMAIL:
        try:
            send_email(subject, message, TO_EMAIL)  # NOTE: no 'body=' kwarg
            print("📧 Email sent.")
        except Exception as e:
            print(f"⚠ Email send failed: {e}\n\nFalling back to stdout below:\n")
            print("\n" + "=" * 60 + "\n" + message + "\n" + "=" * 60 + "\n")
    else:
        print("\n" + "=" * 60 + "\n" + message + "\n" + "=" * 60 + "\n")


if __name__ == "__main__":
    run()