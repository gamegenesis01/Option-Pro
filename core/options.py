# core/options.py
from typing import List, Dict, Any
import yfinance as yf
import pandas as pd

def get_atm_options(
    ticker: str,
    max_dte: int = 10,
    min_dte: int = 2,
    strikes_range: int = 2
) -> List[Dict[str, Any]]:
    """
    Pull near-term options around ATM strike for given ticker.
    Returns list of dicts with option metadata and Greeks from yfinance.
    """
    tk = yf.Ticker(ticker)
    spot = None
    try:
        spot = tk.history(period="1d")["Close"].iloc[-1]
    except Exception:
        return []

    # Get available expirations
    exps = tk.options
    if not exps:
        return []

    out = []
    for exp in exps:
        try:
            # Days to expiry
            dte = (pd.Timestamp(exp) - pd.Timestamp.utcnow().normalize()).days
            if dte < min_dte or dte > max_dte:
                continue

            calls = tk.option_chain(exp).calls
            puts = tk.option_chain(exp).puts

            # Merge calls & puts into one frame with type info
            calls = calls.assign(type="CALL")
            puts = puts.assign(type="PUT")
            df = pd.concat([calls, puts], ignore_index=True)

            # Filter strikes near ATM
            df = df.loc[(df["strike"] >= spot - strikes_range) &
                        (df["strike"] <= spot + strikes_range)]

            # Add DTE
            df = df.assign(DTE=dte)

            # Select needed fields
            for _, row in df.iterrows():
                out.append({
                    "ticker": ticker,
                    "type": row["type"],
                    "strike": float(row["strike"]),
                    "lastPrice": float(row["lastPrice"]),
                    "bid": float(row["bid"]),
                    "ask": float(row["ask"]),
                    "impliedVolatility": float(row["impliedVolatility"]),
                    "delta": float(row.get("delta", 0.0)),
                    "gamma": float(row.get("gamma", 0.0)),
                    "theta": float(row.get("theta", 0.0)),
                    "vega": float(row.get("vega", 0.0)),
                    "rho": float(row.get("rho", 0.0)),
                    "expiration": exp,
                    "spot": float(spot),
                    "DTE": dte
                })
        except Exception:
            continue

    return out
