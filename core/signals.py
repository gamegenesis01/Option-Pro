# core/signals.py
from typing import List, Dict
from core.forecast import forecast_stock_move
from core.options import get_atm_options

def taylor_option_price_change(opt: Dict, price_move: float, iv_change: float, days_forward: int) -> float:
    """
    Approximate change in option price using Taylor Series expansion
    based on given underlying price move, IV change, and days forward.
    """
    delta = opt["delta"]
    gamma = opt["gamma"]
    theta = opt["theta"]
    vega = opt["vega"]
    rho = opt["rho"]

    # Taylor Series expansion:
    change = (
        (delta * price_move) +
        (0.5 * gamma * (price_move ** 2)) +
        (theta * days_forward) +
        (vega * iv_change) +
        (rho * 0)  # ignoring rate change for now
    )
    return change

def generate_trade_ideas(tickers: List[str]) -> List[Dict]:
    """
    For each ticker:
      1. Forecast price move & IV change
      2. Get ATM options within near DTE
      3. Calculate Taylor-estimated price change
      4. Pick top opportunities
    """
    trade_ideas = []

    for t in tickers:
        try:
            forecast = forecast_stock_move(t)
            if not forecast:
                print(f"[{t}] ⚠️ No forecast data.")
                continue

            price_move = forecast["price_move"]
            iv_change = forecast["iv_change"]
            days_forward = forecast["days_forward"]

            options = get_atm_options(t)
            if not options:
                print(f"[{t}] ⚠️ No option data.")
                continue

            for opt in options:
                change = taylor_option_price_change(opt, price_move, iv_change, days_forward)
                if change > 0.5:  # threshold in dollars for trade
                    trade_ideas.append({
                        "ticker": t,
                        "type": opt["type"],
                        "strike": opt["strike"],
                        "expiration": opt["expiration"],
                        "spot": opt["spot"],
                        "est_change": round(change, 2),
                        "lastPrice": opt["lastPrice"],
                        "bid": opt["bid"],
                        "ask": opt["ask"],
                        "iv": opt["impliedVolatility"],
                        "delta": opt["delta"],
                        "gamma": opt["gamma"],
                        "theta": opt["theta"],
                        "vega": opt["vega"]
                    })

        except Exception as e:
            print(f"[{t}] ⚠️ Error: {e}")

    return trade_ideas
