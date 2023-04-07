from build_dataset import TradingDataset
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import math 


dataset_options = {
    "coin_data" : [
        {
            "symbol": "BTCUSDT",
            "exchange": "binance",
            "type": "spot",
        }, 
        {
            "symbol": "ETHUSDT",
            "exchange": "binance",
            "type": "spot",
        },
    ]
}

dataset_client = TradingDataset(
    start_date='2023-04-02',
    end_date='2023-04-08',
    resolution="5min",
    dataset_config=dataset_options
)

dataset_client.build()
df = dataset_client.dataset

scaler = MinMaxScaler()
df['normalized_BTCUSDT_close'] = scaler.fit_transform(df[['BTCUSDT_close']])

scaler = MinMaxScaler()
df['normalized_ETHUSDT_close'] = scaler.fit_transform(df[['ETHUSDT_close']])
ecuildean_distance = []

for i, j in zip(df.normalized_BTCUSDT_close, df.normalized_ETHUSDT_close):
    print(i, j)
    print(math.dist([i], [j]))
    ecuildean_distance.append(math.dist([i], [j]))

df["ecuildean_distance"] = ecuildean_distance

ax = df[['normalized_ETHUSDT_close', 'normalized_BTCUSDT_close', 'ecuildean_distance']].plot(figsize=(10, 6), title='Price Data')
# ax = df[['ETHUSDT_close', 'BTCUSDT_close']].plot(figsize=(10, 6), title='Price Data')

ax.set_xlabel('Date')
ax.set_ylabel('Price')
ax.legend(['ETHUSDT Price', 'BTCUSDT Price', 'Ecuildean Distance'])
plt.show()