import pandas as pd
from download_data import RetrieveCryptoData
import os
import json
import hashlib

# Class to create a dataset which can be analysised or a model can be created
class TradingDataset():

    def __init__(self, start_date: str, end_date: str, resolution: str, dataset_config: dict, build_new: bool = False):
        # Sets up variables for dataset
        self.__setup_variables(start_date, end_date, resolution, dataset_config, build_new)



    def build(self):
        if not self.build_new:
            # If filename exists
            if os.path.exists(self.filename):
                print(f"> Dataset already built. Loading from {self.filename}")
                # Load dataset from filename
                self.df = pd.read_csv(self.filename, index_col=0)
                self.df.index = pd.to_datetime(self.df.index)
                self.dataset = self.df
                return 
            
        if "coin_data" in self.dataset_config:

            for coin_data in self.dataset_config["coin_data"]:
                if coin_data["type"] == "future":
                    self.__add_future_data(coin_data["symbol"], coin_data["exchange"])
                elif coin_data["type"] == "spot":
                    self.__add_spot_data(coin_data["symbol"], coin_data["exchange"])
        print(f"> Dataset has completed building. Length of dataset {len(self.df)}")

        # Save dataset to filename
        self.df.to_csv(self.filename)
        self.dataset = self.df

    def __get_dictionary_hash(self, config):
        hash_obj  = hashlib.md5(json.dumps({k: config[k] for k in sorted(config)}, sort_keys=True).encode())
        return hash_obj.hexdigest()

    def __setup_variables(self, start_date: str, end_date: str, resolution: str, dataset_config: dict, build_new: bool ):
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.start_timestamp = int(self.start_date.timestamp())*1000
        self.end_timestamp = int(self.end_date.timestamp())*1000
        self.resolution = resolution
        self.dataset_config = dataset_config
        self.build_new = build_new

        config_hash = self.__get_dictionary_hash(dataset_config)
        self.filename = f'datasets/{self.start_date.date()}_{self.end_date.date()}_{config_hash}.csv'
        
        self.df = pd.DataFrame(index=pd.date_range(start=self.start_date, end=self.end_date, freq=self.resolution))


    def __add_future_data(self, symbol: str, exchange: str):
        crypto_data_client = RetrieveCryptoData(exchange)
        ohlc_list = crypto_data_client.future_coin_ohlc(self.resolution, self.start_timestamp, self.end_timestamp, symbol)
        data_list = [
            {
                "timestamp": ohlc[0],
                f"{symbol}_open": ohlc[1],
                f"{symbol}_high": ohlc[2],
                f"{symbol}_low": ohlc[3],
                f"{symbol}_close": ohlc[4],
                f"{symbol}_volume": ohlc[5]
            } for ohlc in ohlc_list]
        
        df = pd.DataFrame(data_list)
        df.index = df["timestamp"]
        df.drop(columns=["timestamp"], inplace=True)

        self.df = pd.merge(self.df, df, left_index=True, right_index=True, how='outer')

    
    def __add_spot_data(self, symbol: str, exchange: str):
        crypto_data_client = RetrieveCryptoData(exchange)
        ohlc_list = crypto_data_client.spot_coin_ohlc(self.resolution, self.start_timestamp, self.end_timestamp, symbol)

        data_list = [
            {
                "timestamp": pd.to_datetime(ohlc[0], unit="ms"),
                f"{symbol}_open": ohlc[1],
                f"{symbol}_high": ohlc[2],
                f"{symbol}_low": ohlc[3],
                f"{symbol}_close": ohlc[4],
                f"{symbol}_volume": ohlc[5]
            } for ohlc in ohlc_list]
        
        df = pd.DataFrame(data_list)
        df.index = df["timestamp"]
        df.drop(columns=["timestamp"], inplace=True)

        self.df = pd.merge(self.df, df, left_index=True, right_index=True, how='outer')
    
# Sample on how to use class:
# dataset_options = {
#     "coin_data" : [
#         {
#             "symbol": "BTCUSDT",
#             "exchange": "binance",
#             "type": "spot",
#         }, 
#         {
#             "symbol": "ETHUSDT",
#             "exchange": "binance",
#             "type": "spot",
#         },
#     ]
# }

# TradingDataset(
#     start_date='2023-03-15',
#     end_date='2023-03-16',
#     resolution="5min",
#     dataset_config=dataset_options
# ).build()