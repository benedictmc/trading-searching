import mplfinance as mpf
import json
import pandas as pd
from build_dataset import DeltaDataset

class DeltaStrategy():

    def __init__(self):
        self.dataset = DeltaDataset()
        self.dataset.build()
        self.futures_df = self.dataset.futures_df
        self.move_df = self.dataset.move_df


    def interate_data(self):

        for index, row in self.futures_df.iterrows():
            move_row = self.move_df.loc[index]

            price_diff = row.open - move_row.strike_price
            if price_diff > 0 and price_diff < 100:
                print("Price is within a + 100")
            elif price_diff < 0 and price_diff > -100:
                print("Price is within a - 100")

                

DeltaStrategy().interate_data()
