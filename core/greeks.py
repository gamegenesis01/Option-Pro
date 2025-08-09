# core/greeks.py
import math
from typing import Literal, Dict, Any

OptionType = Literal["call", "put"]


def _phi(x: float) -> float:
    """Standard normal PDF."""
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _Phi(x: float) -> float:
    """Standard normal CDF via error function."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_price_greeks(
    S: float,
    K: float,
    T_years: float,
    r: float,
    iv: float,
    option_type: OptionType = "call",
) -> Dict[str, Any]:
    """
    Black–Scholes price & greeks.

    Inputs:
      S         : underlying price
      K         : strike
      T_years   : time to expiry in YEARS (e.g., days/365)
      r         : risk-free rate (annual, as decimal, e.g. 0.05)
      iv        : implied volatility (annual, as decimal, e.g. 0.22)
      option_type: 'call' or 'put'

    Returns dict with:
      price
      delta
      gamma                     (per $^2)
      theta_per_day            (per day)
      vega_per_1pct            (per +1 vol point, i.e., +0.01)
      rho_per_1pct             (per +1% rate, i.e., +0.01)
      d1, d2

    Notes on units:
      - theta_per_day uses calendar days: theta_annual / 365
      - vega_per_1pct = vega_annual * 0.01
      - rho_per_1pct  = rho_annual  * 0.01
    """
    # Guard rails for numerical stability
    eps = 1e-12
    S = max(S, eps)
    K = max(K, eps)
    T = max(T_years, eps)
    sigma = max(iv, eps)

    # d1, d2
    sig_sqrtT = sigma * math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / sig_sqrtT
    d2 = d1 - sig_sqrtT

    Nd1 = _Phi(d1)
    Nd2 = _Phi(d2)
    nd1 = _phi(d1)

    disc = math.exp(-r * T)

    # Price
    if option_type == "call":
        price = S * Nd1 - K * disc * Nd2
        delta = Nd1
        rho_annual = K * T * disc * Nd2  # per +1.00 (100%) change in r
    else:
        Nmd1 = _Phi(-d1)
        Nmd2 = _Phi(-d2)
        price = K * disc * Nmd2 - S * Nmd1
        delta = Nd1 - 1.0
        rho_annual = -K * T * disc * Nmd2  # per +1.00 (100%) change in r

    # Core greeks (annualized conventions)
    gamma = nd1 / (S * sig_sqrtT)                 # per $^2
    vega_annual = S * nd1 * math.sqrt(T)          # per +1.00 (100) vol (i.e., 1.0)
    theta_annual = (
        -(S * nd1 * sigma) / (2.0 * math.sqrt(T))  # time decay
        - (r * K * disc * (Nd2 if option_type == "call" else _Phi(-d2)))
    )  # per year

    # Convert to friendly intraday units
    theta_per_day = theta_annual / 365.0
    vega_per_1pct = vega_annual * 0.01         # per +0.01 change in iv
    rho_per_1pct = rho_annual * 0.01           # per +0.01 change in r

    return {
        "price": price,
        "delta": delta,
        "gamma": gamma,
        "theta_per_day": theta_per_day,
        "vega_per_1pct": vega_per_1pct,
        "rho_per_1pct": rho_per_1pct,
        "d1": d1,
        "d2": d2,
    }
