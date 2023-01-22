import mplfinance as mpf
import pandas as pd
import time


def chart_ohlc_data(df, rebalance_signal, fifteen_signal):
    # Adds a marker to the chart where the rebalance happened
    add_plots = [
        mpf.make_addplot(rebalance_signal, type='scatter', markersize=150, marker='^', color='r'), 
        mpf.make_addplot(fifteen_signal, type='scatter', markersize=50, marker='*', color='b')
    ]

    mpf.plot(df, type='candle', main_panel=0, addplot=add_plots)



