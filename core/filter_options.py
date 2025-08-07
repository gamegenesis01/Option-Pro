from datetime import datetime, timedelta
import random

def get_best_option(ticker, trade):
    # Mock expiration: 10 days from today
    expiration = (datetime.today() + timedelta(days=10)).strftime("%Y-%m-%d")

    buy_price = round(random.uniform(0.8, 1.5), 2)
    sell_price = round(buy_price * 1.5, 2)
    probability = random.randint(60, 75)

    return {
        "Ticker": trade["Ticker"],
        "Type": trade["Type"],
        "Expiration": expiration,
        "Buy Price": buy_price,
        "Sell Price": sell_price,
        "Profit": round(sell_price - buy_price, 2),
        "Probability": probability
    }
