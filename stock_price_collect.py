"""
daily: to_time = from_time + 1 day
"""

from pandas_datareader import data as pdr
import yfinance as yf


class StockPriceCollector:

    def __init__(self, stock_symbol, from_time, to_time):
        self.from_time = from_time
        self.to_time = to_time
        self.stock_symbol = stock_symbol

        yf.pdr_override()

    def get_price(self):
        price = pdr.get_data_yahoo(
            self.stock_symbol,
            self.from_time,
            self.to_time,
        )['Adj Close']
        price.index = [
            date.strftime('%Y-%m-%d') for date in price.index
        ]
        price = price.fillna(999999)

        return price.to_dict()

    def get_volume(self):
        volume = pdr.get_data_yahoo(
            self.stock_symbol,
            self.from_time,
            self.to_time
        )['Volume']
        volume.index = [
            date.strftime('%Y-%m-%d') for date in volume.index
        ]
        volume = volume.fillna(999999)
        
        return volume.to_dict()
