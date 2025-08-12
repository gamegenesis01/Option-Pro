import yfinance as yf
import pandas as pd

def get_price_history(ticker: str, period: str = "30d", interval: str = "60m") -> pd.DataFrame:
    """
    Fetch OHLCV price history from Yahoo Finance.

    Args:
        ticker: Stock symbol, e.g., "AAPL"
        period: Yahoo period, e.g., "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y"
        interval: e.g., "1m", "5m", "15m", "30m", "60m", "1d"

    Returns:
        DataFrame with datetime index and OHLCV columns.
    """
    try:
        df = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=False)
        if df.empty:
            return None

        df = df.reset_index()
        # Ensure columns match expected
        df.columns = [col.capitalize() if col.lower() != "datetime" else "Datetime" for col in df.columns]
        return df
    except Exception as e:
        print(f"[ERROR] get_price_history failed for {ticker}: {e}")
        return None