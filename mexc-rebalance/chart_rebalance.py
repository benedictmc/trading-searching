import mplfinance as mpf
import pandas as pd
import time
from build_dataset import DatasetBuilder
import matplotlib.pyplot as plt
import numpy as np

ds = DatasetBuilder(start_time=1677715200000, end_time=1677801600000, symbol="APE_USDT")
ds.build_dataset()

def chart_ohlc_data(df, marker_plots=[]):
    colours = ["r", "g", "b", "o"]
    add_plots = []
    
    for i, plot in enumerate(marker_plots):
        # Adds a marker to the chart where the rebalance happened
        add_plots.append(mpf.make_addplot(plot, type='scatter', markersize=150, marker='^', color=colours[i]))

    mpf.plot(df, type='candle', main_panel=0, addplot=add_plots)


def chart_price_data_rebalance(df):
    # print(df.head())

    df.price = df.price.fillna(method='ffill')
    ax = df.plot(y='price')

    rebal_df = df[df['rebal'].notnull()]
    print(rebal_df.head())
    # Add markers at specific dates
    ax.scatter(rebal_df.index.values, rebal_df.price.values, color='red', s=50, zorder=3)

    # Set the axis labels and title
    ax.set_xlabel('Date')
    ax.set_ylabel('Price')
    ax.set_title('Price Timeseries')

    plt.savefig("price_timeseries.png")





chart_price_data_rebalance(ds.dataset)