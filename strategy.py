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

                
            # print(row.open
            # print(move_row.strike_price)
        #     print(index)
        #     print(self.move_df.loc[index])
            # if 
            # open_price = row['open']
            # move_row = move_df.loc[index]
            # print("********")
            # print(f"Time is {index}")
            # # print(f"Open price is {row['open']}")

            # # print(f"Close price is {row['close']}")
            # if open_price >= strike_price:
            #     pass
            #     # print("Price is over strike price")
            # else:
            #     print("***********")
            #     print("Price is UNDER strike price")

            # print(f"Absolute difference in price is: {abs(open_price-strike_price)}, Price of MOVE contract is {move_row['close']}")


# from scipy.optimize import newton

# def implied_volatility(market_price, strike_price):
#     # Define a function to calculate the difference between the
#     # market price and theoretical price of the option
#     def price_difference(sigma):
#         # theoretical_price = calculate_theoretical_price(sigma, underlying_price, strike_price)
#         theoretical_price = (16652.0+16649.0)/2
#         return market_price - theoretical_price
    
#     # Use the Newton-Raphson method to find the root of the price_difference function
#     implied_vol = newton(price_difference, x0=0.2)
    
#     return implied_vol


# market_price = 16649.5
# strike_price = 16700
# iv = implied_volatility(market_price, strike_price )



DeltaStrategy().interate_data()
