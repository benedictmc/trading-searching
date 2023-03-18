from retrieve_data import DataClient
import pandas as pd
import os 
import matplotlib.pyplot as plt
import numpy as np


class DatasetBuilder():

    def __init__(self, start_time, end_time, symbol, from_cache = False, config = {}):
        self.start_time = start_time
        self.end_time = end_time
        self.symbol = symbol
        self.from_cache = from_cache
        self.config = config

        self.data_client = DataClient(start_time, end_time)
        self.file_name = f"{self.symbol}_{self.start_time}_{self.end_time}.csv"
        self.__create_empty_dataset()


    def __create_empty_dataset(self):
        index = pd.date_range(pd.to_datetime(self.start_time, unit='ms'), periods=int((self.end_time - self.start_time)/1000), freq='S')
        self.dataset = pd.DataFrame(index=index)


    def build_dataset(self):
        print(f"> Building {self.symbol} dataset")
        if self.from_cache and self.file_name in os.listdir("datasets"):
            # Load existing dataset
            print("> Loading existing dataset")
            self.dataset = pd.read_csv(f"datasets/{self.file_name}", index_col=0)
        else:
            # Build new dataset 
            print("> Building new dataset")

            # Add price data for symbol
            self.__add_price_data(exchange="mexc")

            # Add price data for additional symbols
            if "price" in self.config:
                for symbol in self.config["price"]:
                    self.__add_price_data(symbol=symbol, exchange="binance")

            # Add rebalance data
            if "rebalance" in self.config:
                self.__add_rebalances()


    def __add_price_data(self, exchange, symbol=None):

        if not symbol:
            symbol = self.symbol

        df = self.data_client.get_future_orderbook_data(symbol=symbol, exchange=exchange)

        df.index = pd.to_datetime(df.timestamp, unit='ms')
        df = df.resample('1S').mean()

        self.dataset = pd.merge(self.dataset, df, left_index=True, right_index=True, how='left')
        self.dataset.drop(columns=["timestamp"], inplace=True)
        self.dataset[f"{symbol}_price"].fillna(method='ffill', inplace=True)



    def __add_rebalances(self):
        rebal_df = self.data_client.get_rebalance_data(self.symbol)

        # Convert to datetime in seconds
        df = pd.DataFrame(index=pd.to_datetime(rebal_df.rebalanceTime, unit='ms').dt.floor('S'))
        df['rebal'] = True
        df = df[~df.index.duplicated(keep='first')]

        self.dataset = pd.merge(self.dataset, df, left_index=True, right_index=True, how='left')


    def save_dataset(self):
        self.dataset.to_csv(f"datasets/{self.file_name}")
