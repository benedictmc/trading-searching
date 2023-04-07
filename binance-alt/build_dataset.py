import pandas as pd
from datetime import datetime
from download_data import RetrieveData
import numpy as np
import ta
import os


class BinanceDataset():

    def __init__(self, start_date: str, end_date: str, symbol: str, build_new: bool = False):
        # Sets up variables for dataset
        self.__setup_variables(start_date, end_date, symbol, build_new)
        self.data_client = RetrieveData()

        print(f'> Building dataset for {self.symbol} from {self.start_date} to {self.end_date}')
        # If the dataset already exists, retrieve it
        if self.dataset_name in os.listdir('data/datasets/') and not build_new:
            print('> Retrieving data from saved dataset')
            self.dataset = pd.read_csv(f"data/datasets/{self.dataset_name}", index_col=0)
        else:
            self.dataset = pd.DataFrame()


    def build(self):
        if not self.dataset.empty and not self.build_new:
            return

        print('> Building new dataset')
        # Starts a basis of a dataset
        self.dataset = self.create_orderflow_features(self.start_timestamp, self.end_timestamp)
        self.create_moving_averages(self.dataset)

        self.create_price_volatility(self.dataset)
        self.create_target_variable(self.dataset)
        self.dataset = self.dataset.dropna()
        self.dataset = self.dataset.round(6)

    
    def save_dataset(self):
        self.dataset.to_csv(f'data/datasets/{self.dataset_name}')


    def __setup_variables(self, start_date: str, end_date: str, symbol: str, build_new: bool):
        self.start_date = start_date
        self.end_date = end_date
        self.build_new = build_new
        # Date to timestamp
        self.start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
        self.end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())
        self.symbol = symbol
        self.resample_period = "1s"
        self.dataset_name = f"{self.symbol}-{self.resample_period}-{self.start_date}-{self.end_date}.csv"


    # Creates a dataframe with the orderflow data
    # Features: price, buy_qty, sell_qty, order_imbalance
    def create_orderflow_features(self, start_ts: int, end_ts: int):
        orderflow_df = self.data_client.get_binance_future_data(self.symbol, start_ts, end_ts)
        orderflow_df.index = pd.to_datetime(orderflow_df.time, unit='ms')
        # Resample the dataframe into 1 second periods, taking the average price
        df = orderflow_df.resample(self.resample_period).agg({'price': 'mean'})

        # Create a new column 'buy_qty' with the sum of qty where is_buyer_maker is True
        df['buy_qty'] = orderflow_df[orderflow_df['is_buyer_maker'] == True].resample(self.resample_period)['qty'].sum()
        df['buy_qty'].fillna(0, inplace=True)

        # Create a new column 'sell_qty' with the sum of qty where is_buyer_maker is False
        df['sell_qty'] = orderflow_df[orderflow_df['is_buyer_maker'] == False].resample(self.resample_period)['qty'].sum()
        df['sell_qty'].fillna(0, inplace=True)

        df['order_imbalance'] = (df['buy_qty'] - df['sell_qty']) / (df['buy_qty'] + df['sell_qty'])
        df['order_imbalance'].fillna(0, inplace=True)

        df.price.fillna(method='ffill', inplace=True)
        
        return df


    # Creates a dataframe with the price volatility
    # Features: price_volatility
    def create_price_volatility(self, df, window=14):
        price_data = df.price
        # Calculate the logarithmic returns
        log_returns = np.log(price_data / price_data.shift(1))

        # Calculate the rolling standard deviation of the returns
        price_volatility = log_returns.rolling(window=window).std()
        df['price_volatility'] = price_volatility

        return price_volatility


    # Creates a dataframe with the price volatility
    # Features: price_volatility
    def create_moving_averages(self, df, close_col="price"):
        # Simple Moving Averages
        df['SMA_short'] = ta.trend.SMAIndicator(df[close_col], window=5).sma_indicator()
        df['SMA_medium'] = ta.trend.SMAIndicator(df[close_col], window=30).sma_indicator()
        df['SMA_long'] = ta.trend.SMAIndicator(df[close_col], window=60).sma_indicator()

        # Exponential Moving Averages
        df['EMA_short'] = ta.trend.EMAIndicator(df[close_col], window=5).ema_indicator()
        df['EMA_medium'] = ta.trend.EMAIndicator(df[close_col], window=30).ema_indicator()
        df['EMA_long'] = ta.trend.EMAIndicator(df[close_col], window=60).ema_indicator()

        # # Weighted Moving Averages
        df['WMA_short'] = ta.trend.WMAIndicator(df[close_col], window=5).wma()
        df['WMA_medium'] = ta.trend.WMAIndicator(df[close_col], window=30).wma()
        df['WMA_long'] = ta.trend.WMAIndicator(df[close_col], window=60).wma()
        return df


    # Creates the target variable
    # Features: price_direction
    def create_target_variable(self, df):
        # Price change in 5 minutes 
        df['price_change'] = df.price.pct_change(periods=300).shift(-300)

        # 1 if price goes up 1% or more, -1 if price goes down 1% or more, 0 if price stays the same
        df['target_variable'] = self.dataset.price_change.apply(lambda x: 1 if x >= 0.025 else 0)
        df.drop(columns=["price_change"],  inplace=True)
        return df['target_variable']
