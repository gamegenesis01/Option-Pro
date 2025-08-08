import yfinance as yf

def get_price_data(ticker, period="2d", interval="5m"):
    """
    Pull intraday data for a ticker.
    Defaults to 5m candles over the last 2 days (good for market open signals).
    """
    df = yf.download(
        ticker,
        period=period,
        interval=interval,
        auto_adjust=True,
        progress=False,
    )
    if df is None or df.empty:
        return df
    df.dropna(inplace=True)
    return df
