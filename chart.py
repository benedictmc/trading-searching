import mplfinance as mpf
from build_dataset import DeltaDataset
import pandas as pd
import time
# import matplotlib.pyplot as plt
# plt.switch_backend('agg')

def chart_Ohlc_data(df, move=None):
    # ts = int(time.time())
    # s = mpf.make_mpf_style(base_mpf_style='charles', rc={'font.size': 6})
    # print(move['close'])
    # ap = mpf.make_addplot(move['close'],color='g',panel=2),  # panel 2 specified
    # df = df.assign(move_close=move['close'])
    print(df.head())
    if move and not move.empty:
        ap2 = [
            mpf.make_addplot(move['close'],color='b',panel=1),  # panel 2 specified
        ]
        mpf.plot(df,type='candle',main_panel=0,addplot=ap2)
    else:
        mpf.plot(df,type='candle')


    # m

dataset = DeltaDataset()
dataset.build()
futures_df = dataset.futures_df
move_df = dataset.move_df
chart_Ohlc_data(move_df)
# chart_Ohlc_data(futures_df, move_df)
