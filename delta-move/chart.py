import mplfinance as mpf
from build_dataset import DeltaDataset
import pandas as pd
import time
from sklearn.preprocessing import MinMaxScaler


def chart_Ohlc_data(df, move=None):
    # if move and not move.empty:
    ap2 = [
        mpf.make_addplot(df['_'],color='b',panel=1),  # panel 2 specified
        mpf.make_addplot(df['close_futures'],color='b',panel=2),  # panel 2 specified

        # mpf.make_addplot(df[''],color='g',panel=1), 
    ]
    mpf.plot(df,type='candle',main_panel=0,addplot=ap2)


dataset = DeltaDataset(start_date='2023-01-01', end_date='2023-01-21')
dataset.build()
df = dataset.df

# trades_df = pd.read_csv("BTCUSDT-trades-2023-01-06_5m.csv", index_col=0)
# trades_df.index = pd.to_datetime(trades_df.index)

# df = df.merge(trades_df, left_index=True, right_index=True, how='inner')

# scaler = MinMaxScaler()
# df["_"] = scaler.fit_transform(df["_"].values.reshape(-1, 1))

# # normalized_income = pd.DataFrame(normalized_income, columns=['Normalized Income'])


# # Create a StandardScaler object


# print(df.head())
chart_Ohlc_data(df)
df.to_csv("test.csv")

# Getting data for Delta with request: https://api.delta.exchange/v2/history/candles?resolution=5m&start=1000000000&end=4000000000&symbol=MV-BTC-23400-111122