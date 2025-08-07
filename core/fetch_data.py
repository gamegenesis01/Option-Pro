import yfinance as yf

def get_price_data(ticker):
    # Fetch 7 days of 15-minute interval OHLCV data
    df = yf.download(ticker, period="7d", interval="15m", auto_adjust=True)
    df.dropna(inplace=True)
    return df
