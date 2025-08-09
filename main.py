# main.py
import os
from datetime import datetime
import pandas as pd

from core.signals import generate_trade_ideas
from core.emailer import send_email

# === CONFIG ===
TICKERS = ["SPY","AAPL","TSLA","MSFT","AMZN","GOOGL","NVDA","META","NFLX","AMD","AAL","PLTR","F","RIVN","SOFI"]
HORIZON_HOURS = int(os.getenv("HORIZON_HOURS", "2"))     # 1-3 hours is your spec
IV_CHANGE_PTS = float(os.getenv("IV_CHANGE_PTS", "0.0"))  # 1.0 = +1 vol point (0.01)
MIN_ROI_PCT   = float(os.getenv("MIN_ROI_PCT", "12.0"))   # only alert if ROI >= this
DTE_MIN       = int(os.getenv("DTE_MIN", "2"))
DTE_MAX       = int(os.getenv("DTE_MAX", "10"))
STRIKES_RANGE = int(os.getenv("STRIKES_RANGE", "2"))

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def format_trade_email(trade_ideas):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    subject = f"Option Pro – {len(trade_ideas)} idea(s) [{ts}]"
    lines = [subject, f"Horizon: {HORIZON_HOURS}h, Δσ: {IV_CHANGE_PTS}pt(s)", ""]
    for t in trade_ideas:
        lines.append(
            f"{t['Ticker']} {t['Type']} {t['Strike']} | Exp {t['Expiration']} | "
            f"Buy ${t['Buy Price']:.2f} → Est Sell ${t['Sell Price']:.2f} | "
            f"ΔOpt ${t['Expected Change']:.2f} | ROI {t['ROI']:.1f}% | "
            f"Δ {t['Delta']:.2f} Γ {t['Gamma']:.3f} Θ {t['Theta']:.3f} V {t['Vega']:.3f}"
        )
    body = "\n".join(lines)
    return subject, body

def format_no_trade_email(reason: str = "No contracts met ROI threshold."):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    subject = f"Option Pro – No Trade Ideas [{ts}]"
    body = (
        f"Bot ran successfully.\n"
        f"Horizon: {HORIZON_HOURS}h, Δσ: {IV_CHANGE_PTS}pt(s)\n"
        f"Reason: {reason}"
    )
    return subject, body

def run():
    log("🔁 Running Option Pro (Greeks/Taylor)…")

    ideas = generate_trade_ideas(
        TICKERS,
        horizon_hours=HORIZON_HOURS,
        iv_change_pts=IV_CHANGE_PTS,
        min_roi_pct=MIN_ROI_PCT,
        dte_min=DTE_MIN,
        dte_max=DTE_MAX,
        strikes_range=STRIKES_RANGE,
    )

    if ideas:
        os.makedirs("output", exist_ok=True)
        pd.DataFrame(ideas).to_csv("output/trade_ideas.csv", index=False)
        subject, body = format_trade_email(ideas)
        send_email(subject, body)
        log(f"✅ Emailed {len(ideas)} idea(s).")
    else:
        subject, body = format_no_trade_email()
        send_email(subject, body)
        log("⚠️ No trade ideas this run (email sent).")

if __name__ == "__main__":
    run()
