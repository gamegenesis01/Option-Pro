def find_trade_opportunity(ticker, signal, df):
    if signal == "bullish":
        return {
            "Ticker": ticker,
            "Type": "Call",
            "Direction": "up"
        }
    elif signal == "bearish":
        return {
            "Ticker": ticker,
            "Type": "Put",
            "Direction": "down"
        }
    return None
