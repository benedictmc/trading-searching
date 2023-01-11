import pandas as pd
import numpy as np

filename = "BTCUSDT-trades-2023-01-07"
df = pd.read_csv(f"{filename}.csv")

# df = df.drop('_')

df['timestamp'] = pd.to_datetime(df['timestamp'],unit='ms')
df.set_index('timestamp', inplace=True)
# df_5s = df.resample('5s').agg({'amount': 'sum', '_': 'count'})
# df_5s.to_csv("5s_trades.csv")

# df_20s = df.resample('20s').agg({'amount': 'sum', '_': 'count'})
# df_20s.to_csv("20s_trades.csv")

# df_40s = df.resample('40s').agg({'amount': 'sum', '_': 'count'})
# df_40s.to_csv("40s_trades.csv")

# df_60s = df.resample('60s').agg({'amount': 'sum', '_': 'count'})
# df_60s.to_csv("60s_trades.csv")

df_5m = df.resample('300s').agg({'amount': 'sum', '_': 'count'})
df_5m.to_csv(f"{filename}_5m.csv")



# print(df_5m.nlargest(20, '_'))
# print(df_5m.nlargest(20, 'amount'))