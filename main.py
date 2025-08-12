# main.py
import os
from datetime import datetime
import pandas as pd

from core.signals import generate_ranked_ideas
from core.emailer import send_email

# ===== Config (env‑tunable) =====
TICKERS = os.getenv(
    "TICKERS",
    "SPY,AAPL,TSLA,MSFT,AMZN,GOOGL,NVDA,META,NFLX,AMD,AAL,PLTR,F,RIVN,SOFI"
).split(",")

HORIZON_HOURS   = int(os.getenv("HORIZON_HOURS", "2"))
IV_CHANGE_PTS   = float(os.getenv("IV_CHANGE_PTS", "0.5"))   # assume +0.5 vol-pt
MIN_SCORE_TIER1 = float(os.getenv("MIN_SCORE_TIER1", "80"))
MIN_SCORE_TIER2 = float(os.getenv("MIN_SCORE_TIER2", "60"))
DTE_MIN         = int(os.getenv("DTE_MIN", "0"))
DTE_MAX         = int(os.getenv("DTE_MAX", "14"))
STRIKES_RANGE   = int(os.getenv("STRIKES_RANGE", "8"))
TOPN_FALLBACK   = int(os.getenv("TOPN_FALLBACK", "5"))
MIN_OI          = int(os.getenv("MIN_OI", "100"))
MAX_SPREAD_PCT  = float(os.getenv("MAX_SPREAD_PCT", "0.35"))

def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _row_line(r):
    return (f"{r['Ticker']} {r['Type']} {r['Strike']} | Exp {r['Expiration']} | "
            f"Mid ${r['Buy Price']:.2f} → Est ${r['Sell Price']:.2f} | "
            f"Δ ${r['Expected Change']:.2f} | ROI {r['ROI']:.1f}% | "
            f"Score {r['Score']:.1f} | Δ {r['Delta']:.2f} Γ {r['Gamma']:.3f} "
            f"Θ {r['Theta']:.4f} V {r['Vega']:.4f} | DTE {r['DTE']} | IV {r['IV']:.3f}")

def _html_table(rows, title):
    if not rows:
        return f"<h3>{title}</h3><p>None</p>"
    th = ("<tr><th>Ticker</th><th>Type</th><th>Strike</th><th>Exp</th>"
          "<th>Mid</th><th>Est</th><th>Δ</th><th>ROI%</th><th>Score</th>"
          "<th>Δ</th><th>Γ</th><th>Θ</th><th>V</th><th>DTE</th><th>IV</th><th>Reasons</th></tr>")
    trs = []
    for r in rows:
        trs.append(
            "<tr>"
            f"<td>{r['Ticker']}</td><td>{r['Type']}</td><td>{r['Strike']}</td>"
            f"<td>{r['Expiration']}</td><td>{r['Buy Price']:.2f}</td>"
            f"<td>{r['Sell Price']:.2f}</td><td>{r['Expected Change']:.2f}</td>"
            f"<td>{r['ROI']:.1f}</td><td>{r['Score']:.1f}</td>"
            f"<td>{r['Delta']:.2f}</td><td>{r['Gamma']:.3f}</td>"
            f"<td>{r['Theta']:.4f}</td><td>{r['Vega']:.4f}</td>"
            f"<td>{r['DTE']}</td><td>{r['IV']:.3f}</td>"
            f"<td>{', '.join(r.get('Reasons', []))}</td>"
            "</tr>"
        )
    return f"<h3>{title}</h3><table border='1' cellpadding='6' cellspacing='0'>{th}{''.join(trs)}</table>"

def run():
    print(f"[{_now()}] 🔁 Running Option Pro (ranked)…")
    result = generate_ranked_ideas(
        tickers=TICKERS,
        horizon_hours=HORIZON_HOURS,
        iv_change_pts=IV_CHANGE_PTS,
        min_score_tier1=MIN_SCORE_TIER1,
        min_score_tier2=MIN_SCORE_TIER2,
        dte_min=DTE_MIN,
        dte_max=DTE_MAX,
        strikes_range=STRIKES_RANGE,
        topN_fallback=TOPN_FALLBACK,
        min_oi=MIN_OI,
        max_spread_pct=MAX_SPREAD_PCT,
    )

    # Save CSV of all ranked rows
    os.makedirs("output", exist_ok=True)
    all_rows = result.get("all", [])
    if all_rows:
        pd.DataFrame(all_rows).to_csv("output/trade_ideas_ranked.csv", index=False)

    # Build email (plain + HTML)
    ts = _now()
    tier1, tier2, watch = result["tier1"], result["tier2"], result["watch"]
    num = len(tier1) + len(tier2)
    subject = f"Option Pro – {num} high-quality idea(s) [{ts}]"

    # Plaintext
    lines = [
        subject,
        f"Cfg: horizon={HORIZON_HOURS}h, dσ={IV_CHANGE_PTS}pt, DTE[{DTE_MIN}-{DTE_MAX}], ±${STRIKES_RANGE},"
        f" minOI={MIN_OI}, maxSpread={int(MAX_SPREAD_PCT*100)}%",
        "",
    ]
    if tier1:
        lines.append("=== Tier 1 ===")
        lines += [_row_line(r) for r in tier1]
        lines.append("")
    if tier2:
        lines.append("=== Tier 2 ===")
        lines += [_row_line(r) for r in tier2]
        lines.append("")
    if watch:
        lines.append("=== Watchlist (Top fallback) ===")
        lines += [_row_line(r) for r in watch]
        lines.append("")
    # Append debug logs
    logs = result.get("logs", [])
    if logs:
        lines.append("--- Debug ---")
        lines += logs

    text_body = "\n".join(lines)

    # HTML
    html = [
        f"<h2>Option Pro – Ranked Ideas</h2>",
        f"<p><b>{ts}</b></p>",
        f"<p><code>horizon={HORIZON_HOURS}h, dσ={IV_CHANGE_PTS}pt, DTE[{DTE_MIN}-{DTE_MAX}], ±${STRIKES_RANGE}, "
        f"minOI={MIN_OI}, maxSpread={int(MAX_SPREAD_PCT*100)}%</code></p>",
        _html_table(tier1, "Tier 1 (High Conviction)"),
        _html_table(tier2, "Tier 2 (Moderate)"),
        _html_table(watch, "Watchlist (Top Fallback)"),
        "<h4>Debug</h4><pre>" + "\n".join(logs) + "</pre>"
    ]
    html_body = "\n".join(html)

    send_email(subject, text_body, html_body=html_body)

    if num:
        print(f"[{_now()}] ✅ Emailed {num} idea(s). Saved output/trade_ideas_ranked.csv")
    else:
        print(f"[{_now()}] ⚠️ No Tier1/Tier2 ideas; emailed watchlist/debug.")

if __name__ == "__main__":
    run()