# core/scoring.py

from typing import List, Dict

def score_contracts(contracts: List[Dict]) -> List[Dict]:
    """
    Assigns a score to each option contract based on a mix of liquidity and volatility factors.

    Parameters
    ----------
    contracts : List[Dict]
        List of option contract dictionaries.
        Expected keys: 'volume', 'openInterest', 'impliedVolatility', 'lastPrice'

    Returns
    -------
    List[Dict]
        The same list of contracts, each with an added 'score' key.
    """
    
    scored = []
    for c in contracts:
        try:
            volume = float(c.get("volume", 0))
            oi = float(c.get("openInterest", 0))
            iv = float(c.get("impliedVolatility", 0))
            price = float(c.get("lastPrice", 0))

            # Basic scoring formula (you can tune weights later)
            score = (
                (volume / 1000) * 0.4 +     # liquidity weight
                (oi / 1000) * 0.3 +         # open interest weight
                (iv * 100) * 0.2 +          # implied volatility weight
                (price / 10) * 0.1          # price factor
            )

            c["score"] = round(score, 2)
            scored.append(c)

        except Exception:
            # Skip any contract with bad/missing data
            continue

    # Sort contracts from highest to lowest score
    scored.sort(key=lambda x: x.get("score", 0), reverse=True)

    return scored