import pandas as pd
from datetime import datetime
from download_data import RetrieveDeltaData
import numpy as np

class DeltaDataset():

    def __init__(self):

        # Sets up variables for dataset
        self.__setup_variables()
        self.delta_client = RetrieveDeltaData()


    def build(self):
        # self.move_df = self.__get_move_data(self.start_time, self.end_time)
        self.futures_df = self.__get_future_data(self.start_time, self.end_time)
        self.move_df = self.__get_move_data(self.start_time, self.end_time)

        self.futures_df.to_csv("futures.csv")
        self.move_df.to_csv("move.csv")


    def __setup_variables(self):
        self.start_time = '2022-12-25'
        # self.end_time = '2022-12-31'

        # self.start_time = '2023-01-04'
        self.end_time = '2023-01-05'
        self.period = "5m"


    def __get_future_data(self, start_date, end_date):
        start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
        end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())
        ohlc_list = self.delta_client.get_delta_data(self.period, start_timestamp, end_timestamp, "BTCUSDT")
        future_df = pd.DataFrame(ohlc_list)
        future_df = future_df.set_index('time')
        future_df.index = pd.to_datetime(future_df.index, unit="s")
        return future_df


    def __get_move_data(self, start_date, end_date):
        start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
        end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())
        move_list = self.delta_client.get_delta_move_data(self.period, start_timestamp, end_timestamp, "BTC")

        move = pd.DataFrame(move_list)
        move = move.set_index('time')
        move.index = pd.to_datetime(move.index, unit="s")
        return move

