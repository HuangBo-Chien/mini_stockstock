import mplfinance as mpf
import pandas as pd
from Data import getData
from datetime import date
from matplotlib import pyplot as plt

def plot_stock_price(data_df: pd.DataFrame):
    """
    plot stock price. The format of data_df follows yfinance.
    """
    ll = len(data_df.keys())
    Ticker_idx, _ = next(filter(lambda x: x[1] == "Ticker", enumerate(data_df.keys().names)), (-1, None))
    unique_ticker = set()
    for one_columns in data_df.keys():
        unique_ticker.add(one_columns[Ticker_idx])
    for one_ticker in unique_ticker:
        mpf.plot(data = data_df.xs(key = one_ticker, level = 1, axis = 1))

if __name__ == "__main__":
    data = getData(
        StockID = "0050.TW",
        Start_Date = date(2020, 1, 1),
        End_Date = date(2021, 1, 1)
    )
    plot_stock_price(data_df = data)
    # data.xs(key = "0050.TW", level = 1, axis = 1)
    # mpf.plot(data = data.xs(key = "0050.TW", level = 1, axis = 1))