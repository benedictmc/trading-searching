from retrieve_data import DataClient
import pandas as pd
import os 
import matplotlib.pyplot as plt
import numpy as np


class DatasetBuilder():

    def __init__(self, start_time, end_time, symbol):
        self.start_time = start_time
        self.end_time = end_time
        self.symbol = symbol
        self.data_client = DataClient(start_time, end_time)
        self.file_name = f"{self.symbol}_{self.start_time}_{self.end_time}.csv"

        index = pd.date_range(pd.to_datetime(self.start_time, unit='ms'), periods=86400, freq='S')
        self.dataset = pd.DataFrame(index=index)


    def build_dataset(self):

        # if False and self.file_name in os.listdir("datasets"):
        if self.file_name in os.listdir("datasets"):

            # Load existing dataset
            print("Loading existing dataset")
            self.dataset = pd.read_csv(f"datasets/{self.file_name}")
        else:
            # Build new dataset 
            print("Building new dataset")
            self.__add_price_data()
            self.__add_rebalances()
    

    def __add_price_data(self):
        df = self.data_client.get_future_orderbook_data(self.symbol)
        print(df.head())

        df.index = pd.to_datetime(df.timestamp, unit='ms')
        df = df.resample('1S').mean()
        df = df.drop(columns=["timestamp"])

        self.dataset = pd.merge(self.dataset, df, left_index=True, right_index=True, how='left')
        self.dataset.price.fillna(method='ffill', inplace=True)



    def __add_rebalances(self):
        rebal_df = self.data_client.get_rebalance_data(self.symbol)

        # Convert to datetime in seconds
        df = pd.DataFrame(index=pd.to_datetime(rebal_df.rebalanceTime, unit='ms').dt.floor('S'))
        df['rebal'] = True
        df = df[~df.index.duplicated(keep='first')]

        self.dataset = pd.merge(self.dataset, df, left_index=True, right_index=True, how='left')


    def save_dataset(self):
        self.dataset.to_csv(f"datasets/{self.file_name}")


ds = DatasetBuilder(start_time=1677715200000, end_time=1677801600000, symbol="APE_USDT")
ds.build_dataset()
# ds.save_dataset()
# exit()
data = {
    'buy_price': [], 
    'rebal': []
}
last_sell_price, last_buy_price = 5, 5

for index, row in ds.dataset.iterrows():
    data["buy_price"].append(row.price)

    if row.rebal == True:
        data["rebal"].append(row.price)
    else:
        data["rebal"].append(np.nan)

    # if row["T"] == 1:
    #     data["sell_price"].append(row.price)
    #     last_sell_price = row.price
    # else:
    #     data["sell_price"].append(last_sell_price)

    # if row["T"] == 2:
    #     data["buy_price"].append(row.price)
    #     last_buy_price = row.price
    # else:
    #     data["buy_price"].append(last_buy_price)


df = pd.DataFrame(data, index=ds.dataset.index)

# plot the price column against the time column
ax = df.plot(y='buy_price', figsize=(10, 5))
df.plot(y='rebal', ax=ax)

plt.xlabel('Time')
plt.ylabel('Price')
plt.title('Price over Time')
plt.savefig('foo.png')