import mplfinance as mpf
import pandas as pd
import time
from build_dataset import DatasetBuilder
import matplotlib.pyplot as plt
import numpy as np



def chart_ohlc_data(df, marker_plots=[]):
    colours = ["r", "g", "b", "o"]
    add_plots = []
    
    for i, plot in enumerate(marker_plots):
        # Adds a marker to the chart where the rebalance happened
        add_plots.append(mpf.make_addplot(plot, type='scatter', markersize=150, marker='^', color=colours[i]))

    mpf.plot(df, type='candle', main_panel=0, addplot=add_plots)


def chart_price_data_rebalance(df, symbol):

    # Fill in missing price data
    df.price = df.price.fillna(method='ffill')
    fig, ax = plt.subplots(figsize=(15, 7))
    df.index = pd.to_datetime(df.index.values, format="%Y-%m-%d %H:%M:%S")

    df.plot(y='price', ax=ax)
    
    rebal_df = df[df['rebal'].notnull()]

    if len(rebal_df.price.values) > 1:
        print("*******************")
        print(symbol)
        print("*******************")

    ax.scatter(rebal_df.index.values, rebal_df.price.values, color='red', s=50, zorder=3)

    # Set the axis labels and title
    ax.set_xlabel('Date')
    ax.set_ylabel('Price')
    ax.set_title('Price Timeseries')
    
    plt.savefig(f"charts/{symbol}_{1677801600000}_{1677888000000}.png")



for symbol in ["DENT_USDT","SNFT_USDT","KAVA_USDT","IOST_USDT","SLP_USDT","COTI_USDT","BTS_USDT","GLMR_USDT","AVAX_USDT","RUNE_USDT","DAR_USDT","SPELL_USDT","KSM_USDT","JASMY_USDT","MKR_USDT","CTSI_USDT","FLOW_USDT","CHZ_USDT","DYDX_USDT","RSR_USDT","CHR_USDT","ACH_USDT","AAVE_USDT","ONE_USDT","FTM_USDT","ROSE_USDT","PERP_USDT","ANC_USDT","GAL_USDT","EOS_USDT","OP_USDT","STORJ_USDT","REEF_USDT","ALGO_USDT","STEPN_USDT","LAZIO_USDT","MATIC_USDT","LINA_USDT","WOO_USDT","ENS_USDT","VINU_USDT","MANA_USDT","RAY_USDT","LINK_USDT","KNC_USDT","BLZ_USDT","YFII_USDT","NKN_USDT","LUNA_USDT","XEM_USDT","HT_USDT","ZEN_USDT","BAL_USDT","SFP_USDT"]:
    ds = DatasetBuilder(start_time=1677801600000, end_time=1677888000000, symbol=symbol)
    ds.build_dataset()
    ds.save_dataset()
    chart_price_data_rebalance(ds.dataset, symbol)