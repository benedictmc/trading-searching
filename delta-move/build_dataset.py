import pandas as pd
from datetime import datetime
from download_data import RetrieveDeltaData

class DeltaDataset():

    def __init__(self, start_date: str, end_date: str):

        # Sets up variables for dataset
        self.__setup_variables(start_date, end_date)
        self.delta_client = RetrieveDeltaData()


    def build(self):
        self.move_df = self.__get_move_data(self.start_time, self.end_time)
        
        #  Bases in futures stand and end by the dates of the MOVE data
        # move_start_ts = int(self.move_df.index[0].timestamp())
        # move_end_ts = int(self.move_df.index[-1].timestamp()) + 5*60
        # self.futures_df = self.__get_future_data(move_start_ts, move_end_ts)

        self.df = self.move_df.merge(self.futures_df, left_index=True, right_index=True, how='inner')


    def __setup_variables(self, start_date: str, end_date: str):
        self.start_time = start_date
        self.end_time = end_date
        self.coin = "BTC"
        self.period = "1m"


    # Takes int timestamps
    def __get_future_data(self, start_ts: int, end_ts: int):
        ohlc_list = self.delta_client.get_delta_data(self.period, start_ts, end_ts, "BTCUSDT")
        future_df = pd.DataFrame(ohlc_list)
        future_df = future_df.set_index('time')
        future_df.index = pd.to_datetime(future_df.index, unit="s")
        future_df.rename(columns = {'open': 'open_futures', 'close': 'close_futures', 'high': 'high_futures', 'low': 'low_futures'}, inplace=True)
        return future_df

    # Takes date string
    def __get_move_data(self, start_date: str, end_date: str):
        start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
        end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())
        move_list = self.delta_client.get_delta_move_data(self.period, self.coin, start_timestamp, end_timestamp)
        move = pd.DataFrame(move_list)
        
        if move.empty:
            return move

        move = move.set_index('time')
        move.index = pd.to_datetime(move.index, unit="s")
        return move




# DeltaDataset(start_date='2023-03-15', end_date='2023-03-16').build()