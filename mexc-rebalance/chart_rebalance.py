import mplfinance as mpf
import pandas as pd
import time


def chart_ohlc_data(df, marker_plots=[]):
    colours = ["r", "g", "b", "o"]
    add_plots = []
    
    for i, plot in enumerate(marker_plots):
        # Adds a marker to the chart where the rebalance happened
        add_plots.append(mpf.make_addplot(plot, type='scatter', markersize=150, marker='^', color=colours[i]))

    mpf.plot(df, type='candle', main_panel=0, addplot=add_plots)



