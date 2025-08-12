# core/filter_options.py

from typing import List, Dict

def filter_contracts(contracts: List[Dict], min_volume: int = 100, max_spread: float = 0.05) -> List[Dict]:
    """
    Filters option contracts based on minimum volume and maximum bid-ask spread.

    Parameters
    ----------
    contracts : List[Dict]
        List of option contract dictionaries.
        Each dict should have at least 'volume', 'bid', and 'ask' keys.
    min_volume : int
        Minimum acceptable trading volume for a contract.
    max_spread : float
        Maximum acceptable bid-ask spread as a fraction of the ask price.

    Returns
    -------
    List[Dict]
        Filtered list of contracts that meet the criteria.
    """

    filtered = []
    for c in contracts:
        try:
            bid = float(c.get("bid", 0))
            ask = float(c.get("ask", 0))
            vol = int(c.get("volume", 0))

            if vol < min_volume:
                continue

            if ask <= 0:
                continue

            spread = (ask - bid) / ask
            if spread > max_spread:
                continue

            filtered.append(c)

        except Exception:
            # Skip any contract with bad data
            continue

    return filtered