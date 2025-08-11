# main.py
import os
from datetime import datetime
import pandas as pd

from core.signals import generate_trade_ideas
from core.emailer import send_email

# === CONFIG ===
TICKERS = ["SPY","AAPL","TSLA","MSFT","AMZN","GOOGL","NVDA","META","NFLX","AMD","AAL","PLTR","F","RIVN","SOFI"]

# Tunables via env (good defaults to *see* flow)
HORIZON_HOURS = int(os.getenv("HORIZON_HOURS", "2"))      # 1–3 hours
IV_CHANGE_PTS = float(os.getenv("IV_CHANGE_PTS", "0.5"))   # assume +0.5 vol-pt (0.005) by default
MIN_ROI_PCT   = float(os.getenv("MIN_ROI_PCT", "8.0"))     # start at 8% to confirm ideas flow
DTE_MIN       = int(os.getenv("DTE_MIN", "1"))             # include short-dated
DTE_MAX       = int(os.getenv("DTE_MAX", "14"))
STRIKES_RANGE = int(os.getenv("STRIKES_RANGE", "5"))       # ±$5 around spot

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def format_trade_email(trade_ideas):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    subject = f"Option Pro – {len(trade_ideas)} idea(s) [{ts}]"
    lines = [subject, f"Horizon: {HORIZON_HOURS}h, Δσ: {IV_CHANGE_PTS}pt(s), Min ROI: {MIN_ROI_PCT}%", ""]
    for t in trade_ideas:
        lines.append(
            f"{t['Ticker']} {t['Type']} {t['Strike']} | Exp {t['Expiration']} | "
            f"Buy ${t['Buy Price']:.2f} → Est Sell ${t['Sell Price']:.2f} | "
            f"ΔOpt ${t['Expected Change']:.2f} | ROI {t['ROI']:.1f}% | "
            f"Δ {t['Delta']:.2f} Γ {t['Gamma']:.3f} Θ {t['Theta']:.4f} V {t['Vega']:.4f} | DTE {t['DTE']} | IV {t['IV']:.3f}"
        )
    body = "\n".join(lines)
    return subject, body

def format_no_trade_email(reason: str = "No contracts met ROI threshold."):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    subject = f"Option Pro – No Trade Ideas [{ts}]"
    body = (
        f"Bot ran successfully.\n"
        f"Horizon: {HORIZON_HOURS}h, Δσ: {IV_CHANGE_PTS}pt(s), Min ROI: {MIN_ROI_PCT}%\n"
        f"Reason: {reason}\n"
        f"(Tip: temporarily set MIN_ROI_PCT=6–8 and IV_CHANGE_PTS=0.5–1.0 to see more flow.)"
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
        df = pd.DataFrame(ideas)
        df.to_csv("output/trade_ideas.csv", index=False)
        subject, body = format_trade_email(ideas)
        send_email(subject, body)
        log(f"✅ Emailed {len(ideas)} idea(s). Saved output/trade_ideas.csv")
    else:
        subject, body = format_no_trade_email()
        send_email(subject, body)
        log("⚠️ No trade ideas this run (email sent).")

if __name__ == "__main__":
    run()
